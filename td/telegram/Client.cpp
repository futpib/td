//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2026
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Client.h"
#include "td/telegram/ClientInternal.h"
#include "td/telegram/net/NetQueryDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdCallback.h"

#include "td/actor/actor.h"
#include "td/actor/ConcurrentScheduler.h"

#include "td/utils/algorithm.h"
#include "td/utils/common.h"
#include "td/utils/crypto.h"
#include "td/utils/ExitGuard.h"
#include "td/utils/FlatHashMap.h"
#include "td/utils/FlatHashSet.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/MpscPollableQueue.h"
#include "td/utils/port/config.h"
#include "td/utils/port/RwMutex.h"
#include "td/utils/port/thread.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/tl_parsers.h"
#include "td/utils/utf8.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>

namespace td {

static GlobalExternalDispatchCallback global_external_dispatch_;
static std::mutex global_external_dispatch_mutex_;

void set_external_dispatch(GlobalExternalDispatchCallback callback) {
  std::lock_guard<std::mutex> lock(global_external_dispatch_mutex_);
  global_external_dispatch_ = std::move(callback);
}

static GlobalExternalDispatchCallback get_global_external_dispatch() {
  std::lock_guard<std::mutex> lock(global_external_dispatch_mutex_);
  return global_external_dispatch_;
}

// Externally-dispatched NetQueries are stashed here, on the scheduler thread
// that dispatched them, and never handed to the embedder.  The embedder only
// learns the id; completion looks the query back up on a scheduler thread.
//
// We also remember which scheduler dispatched the query.  NetQueryDispatcher::
// dispatch() runs synchronously on the originating actor's scheduler, so the
// query's cancel_slot (and usually its callback) actor live on that scheduler.
// The completion must run there too: NetQuery::start_migrate migrates the
// cancel_slot, and delivering the finished query from any other scheduler would
// migrate that actor as if it lived on the completing scheduler, underflowing
// Scheduler::actor_count_.
static std::mutex external_queries_mutex_;
struct StashedExternalQuery {
  int32 sched_id = 0;
  NetQueryPtr query;
};
static FlatHashMap<uint64, StashedExternalQuery> external_queries_;
static uint64 external_queries_next_id_ = 1;

static ExternalQuery stash_external_query(NetQueryPtr query) {
  ExternalQuery data;
  data.query = query->query().as_slice().str();
  data.gzip = (query->gzip_flag() == NetQuery::GzipFlag::On);
  data.raw_dc_id = query->dc_id().is_main() ? 0 : query->dc_id().get_raw_id();
  data.type = static_cast<int32>(query->type());
  std::lock_guard<std::mutex> lock(external_queries_mutex_);
  data.id = external_queries_next_id_++;
  auto &slot = external_queries_[data.id];
  slot.sched_id = Scheduler::instance()->sched_id();
  slot.query = std::move(query);
  return data;
}

static NetQueryPtr take_external_query(uint64 id) {
  std::lock_guard<std::mutex> lock(external_queries_mutex_);
  auto it = external_queries_.find(id);
  if (it == external_queries_.end()) {
    return {};
  }
  auto query = std::move(it->second.query);
  external_queries_.erase(it);
  return query;
}

static int32 external_query_sched(uint64 id) {
  std::lock_guard<std::mutex> lock(external_queries_mutex_);
  auto it = external_queries_.find(id);
  return (it == external_queries_.end()) ? -1 : it->second.sched_id;
}

namespace {
// Finishes an externally-dispatched query on the scheduler that dispatched it
// (created there via create_actor_on_scheduler).  Takes the stashed NetQuery,
// applies the result and delivers it to its callback -- all on that scheduler,
// where the query's actors live, so no cross-scheduler migration is forced.
class ExternalCompletionActor final : public Actor {
 public:
  ExternalCompletionActor(uint64 query_id, ActorId<Td> td, ExternalQueryResult result)
      : query_id_(query_id), td_(td), result_(std::move(result)) {
  }

 private:
  uint64 query_id_;
  ActorId<Td> td_;
  ExternalQueryResult result_;

  void start_up() final {
    auto query = take_external_query(query_id_);
    if (!query.empty()) {
      if (result_.is_ok) {
        query->set_ok(BufferSlice(Slice(result_.ok_data.data(), result_.ok_data.size())));
      } else {
        query->set_error(Status::Error(result_.error_code, result_.error_message));
      }
      auto callback = query->move_callback();
      if (callback.empty()) {
        send_closure_later(td_, &Td::on_result, std::move(query));
      } else {
        send_closure_later(std::move(callback), &NetQueryCallback::on_result, std::move(query));
      }
    }
    stop();
  }
};
}  // namespace

#if TD_THREAD_UNSUPPORTED || TD_EVENTFD_UNSUPPORTED
class TdReceiver {
 public:
  ClientManager::Response receive(double timeout) {
    if (!responses_.empty()) {
      auto result = std::move(responses_.front());
      responses_.pop();
      return result;
    }
    return {0, 0, nullptr};
  }

  unique_ptr<TdCallback> create_callback(ClientManager::ClientId client_id) {
    class Callback final : public TdCallback {
     public:
      Callback(ClientManager::ClientId client_id, TdReceiver *impl) : client_id_(client_id), impl_(impl) {
      }
      void on_result(uint64 id, td_api::object_ptr<td_api::Object> result) final {
        impl_->responses_.push({client_id_, id, std::move(result)});
      }
      void on_error(uint64 id, td_api::object_ptr<td_api::error> error) final {
        impl_->responses_.push({client_id_, id, std::move(error)});
      }
      Callback(const Callback &) = delete;
      Callback &operator=(const Callback &) = delete;
      Callback(Callback &&) = delete;
      Callback &operator=(Callback &&) = delete;
      ~Callback() final {
        impl_->responses_.push({client_id_, 0, nullptr});
      }

     private:
      ClientManager::ClientId client_id_;
      TdReceiver *impl_;
    };
    return td::make_unique<Callback>(client_id, this);
  }

  void add_response(ClientManager::ClientId client_id, uint64 id, td_api::object_ptr<td_api::Object> result) {
    responses_.push({client_id, id, std::move(result)});
  }

 private:
  std::queue<ClientManager::Response> responses_;
};

class ClientManager::Impl final {
 public:
  ClientId create_client_id() {
    CHECK(client_id_ != std::numeric_limits<ClientId>::max());
    auto client_id = ++client_id_;
    pending_clients_.insert(client_id);
    return client_id;
  }

  void send(ClientId client_id, RequestId request_id, td_api::object_ptr<td_api::Function> &&request) {
    if (pending_clients_.erase(client_id) != 0) {
      if (tds_.empty()) {
        CHECK(concurrent_scheduler_ == nullptr);
        CHECK(options_.net_query_stats == nullptr);
        options_.net_query_stats = std::make_shared<NetQueryStats>();
        global_dispatch_ = get_global_external_dispatch();
        concurrent_scheduler_ = make_unique<ConcurrentScheduler>(0, 0);
        concurrent_scheduler_->start();
      }
      auto options = options_;
      if (global_dispatch_) {
        auto dispatch = global_dispatch_;
        auto id = client_id;
        options.external_dispatch = [dispatch, id](NetQueryPtr query) {
          dispatch(id, stash_external_query(std::move(query)));
        };
      }
      tds_[client_id] =
          concurrent_scheduler_->create_actor_unsafe<Td>(0, "Td", receiver_.create_callback(client_id), options);
    }
    requests_.push_back({client_id, request_id, std::move(request)});
  }

  Response receive(double timeout) {
    if (!requests_.empty()) {
      for (size_t i = 0; i < requests_.size(); i++) {
        auto &request = requests_[i];
        if (request.client_id <= 0 || request.client_id > client_id_) {
          receiver_.add_response(request.client_id, request.id,
                                 td_api::make_object<td_api::error>(400, "Invalid TDLib instance specified"));
          continue;
        }
        auto it = tds_.find(request.client_id);
        if (it == tds_.end() || it->second.empty()) {
          receiver_.add_response(request.client_id, request.id,
                                 td_api::make_object<td_api::error>(500, "Request aborted"));
          continue;
        }

        CHECK(concurrent_scheduler_ != nullptr);
        auto guard = concurrent_scheduler_->get_main_guard();
        send_closure_later(it->second, &Td::request, request.id, std::move(request.request));
      }
      requests_.clear();
    }

    auto response = receiver_.receive(0);
    if (response.client_id == 0 && response.request_id == 0 && concurrent_scheduler_ != nullptr) {
      concurrent_scheduler_->run_main(0);
      response = receiver_.receive(0);
    } else {
      ConcurrentScheduler::emscripten_clear_main_timeout();
    }
    if (response.request_id == 0 && response.object != nullptr &&
        response.object->get_id() == td_api::updateAuthorizationState::ID &&
        static_cast<const td_api::updateAuthorizationState *>(response.object.get())->authorization_state_->get_id() ==
            td_api::authorizationStateClosed::ID) {
      CHECK(concurrent_scheduler_ != nullptr);
      auto guard = concurrent_scheduler_->get_main_guard();
      auto it = tds_.find(response.client_id);
      CHECK(it != tds_.end());
      it->second.reset();

      response.client_id = 0;
      response.object = nullptr;
    }
    if (response.object == nullptr && response.client_id != 0 && response.request_id == 0) {
      auto it = tds_.find(response.client_id);
      CHECK(it != tds_.end());
      CHECK(it->second.empty());
      tds_.erase(it);

      response.object = td_api::make_object<td_api::updateAuthorizationState>(
          td_api::make_object<td_api::authorizationStateClosed>());

      if (tds_.empty()) {
        CHECK(options_.net_query_stats.use_count() == 1);
        CHECK(options_.net_query_stats->get_count() == 0);
        options_.net_query_stats = nullptr;
        concurrent_scheduler_->finish();
        concurrent_scheduler_ = nullptr;
        reset_to_empty(tds_);
      }
    }
    return response;
  }

  Impl() = default;
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;
  ~Impl() {
    if (concurrent_scheduler_ == nullptr) {
      return;
    }

    {
      auto guard = concurrent_scheduler_->get_main_guard();
      for (auto &td : tds_) {
        td.second.reset();
      }
    }
    while (!tds_.empty() && !ExitGuard::is_exited()) {
      receive(0.1);
    }
    if (concurrent_scheduler_ != nullptr) {
      concurrent_scheduler_->finish();
    }
  }

 private:
  TdReceiver receiver_;
  struct Request {
    ClientId client_id;
    RequestId id;
    td_api::object_ptr<td_api::Function> request;
  };
  vector<Request> requests_;
  unique_ptr<ConcurrentScheduler> concurrent_scheduler_;
  ClientId client_id_{0};
  Td::Options options_;
  GlobalExternalDispatchCallback global_dispatch_;
  FlatHashSet<int32> pending_clients_;
  FlatHashMap<int32, ActorOwn<Td>> tds_;
};

class Client::Impl final {
 public:
  Impl() : client_id_(impl_.create_client_id()) {
  }

  void send(Request request) {
    impl_.send(client_id_, request.id, std::move(request.function));
  }

  Response receive(double timeout) {
    auto response = impl_.receive(timeout);

    Response old_response;
    old_response.id = response.request_id;
    old_response.object = std::move(response.object);
    return old_response;
  }

 private:
  ClientManager::Impl impl_;
  ClientManager::ClientId client_id_;
};

#else

class MultiTd final : public Actor {
 public:
  MultiTd(Td::Options options, GlobalExternalDispatchCallback global_dispatch)
      : options_(std::move(options)), global_dispatch_(std::move(global_dispatch)) {
    options_.external_dispatch = {};
  }
  void create(int32 td_id, unique_ptr<TdCallback> callback) {
    auto &td = tds_[td_id];
    CHECK(td.empty());

    auto options = options_;
    if (global_dispatch_) {
      auto dispatch = global_dispatch_;
      auto id = td_id;
      options.external_dispatch = [dispatch, id](NetQueryPtr query) {
        dispatch(id, stash_external_query(std::move(query)));
      };
    }

    string name = "Td";
    auto context = std::make_shared<ActorContext>();
    auto old_context = set_context(context);
    auto old_tag = set_tag(to_string(td_id));
    td = create_actor<Td>("Td", std::move(callback), options);
    set_context(std::move(old_context));
    set_tag(std::move(old_tag));
  }

  void send(ClientManager::ClientId client_id, ClientManager::RequestId request_id,
            td_api::object_ptr<td_api::Function> &&request) {
    auto &td = tds_[client_id];
    CHECK(!td.empty());
    send_closure(td, &Td::request, request_id, std::move(request));
  }

  void close(int32 td_id) {
    size_t erased_count = tds_.erase(td_id);
    CHECK(erased_count > 0);
  }

  void complete_external(int32 td_id, uint64 query_id, ExternalQueryResult result) {
    // Route the completion to the scheduler that dispatched the query, where its
    // cancel_slot/callback actors live.  Finishing it on this scheduler instead
    // would migrate those actors as if from here and underflow actor_count_.
    auto sched_id = external_query_sched(query_id);
    if (sched_id < 0) {
      return;
    }
    auto it = tds_.find(td_id);
    auto td = (it != tds_.end()) ? it->second.get() : ActorId<Td>();
    create_actor_on_scheduler<ExternalCompletionActor>(
        "ExternalCompletion", sched_id, query_id, td, std::move(result))
        .release();
  }

  void push_updates(int32 td_id, std::string updates_bytes) {
    // Inbound counterpart to external_dispatch: parse the embedder-supplied
    // update bytes and feed them to the client's UpdatesManager, exactly as a
    // native Session would after reading them off the wire.  No NetQuery and no
    // actor ownership crosses here, so (unlike completion) there is nothing to
    // migrate -- we just hand a plain telegram_api object to the Td actor.
    auto it = tds_.find(td_id);
    if (it == tds_.end() || it->second.empty()) {
      return;
    }
    auto buffer = BufferSlice(Slice(updates_bytes.data(), updates_bytes.size()));
    TlBufferParser parser(&buffer);
    auto updates = telegram_api::Updates::fetch(parser);
    parser.fetch_end();
    if (parser.get_error()) {
      LOG(ERROR) << "Failed to fetch externally-pushed update: " << parser.get_error();
      return;
    }
    // auth_key_id 0: these updates arrive over the embedder's connection, not a
    // TDLib-owned auth key; on_update_from_auth_key_id(0) is a no-op by design.
    send_closure_later(it->second, &Td::on_update, std::move(updates), static_cast<uint64>(0));
  }

 private:
  Td::Options options_;
  GlobalExternalDispatchCallback global_dispatch_;
  FlatHashMap<int32, ActorOwn<Td>> tds_;
};

class TdReceiver {
 public:
  TdReceiver() {
    output_queue_ = std::make_shared<OutputQueue>();
    output_queue_->init();
  }

  ClientManager::Response receive(double timeout, bool from_manager) {
    VLOG(td_requests) << "Begin to wait for updates with timeout " << timeout;
    auto is_locked = receive_lock_.exchange(true);
    if (is_locked) {
      if (from_manager) {
        LOG(FATAL) << "Receive must not be called simultaneously from two different threads, but this has just "
                      "happened. Call it from a fixed thread, dedicated for updates and response processing.";
      } else {
        LOG(FATAL) << "Receive is called after Client destroy, or simultaneously from different threads";
      }
    }
    auto response = receive_unlocked(clamp(timeout, 0.0, 1000000.0));
    is_locked = receive_lock_.exchange(false);
    CHECK(is_locked);
    VLOG(td_requests) << "End to wait for updates, returning object " << response.request_id << ' '
                      << response.object.get();
    return response;
  }

  unique_ptr<TdCallback> create_callback(ClientManager::ClientId client_id) {
    class Callback final : public TdCallback {
     public:
      Callback(ClientManager::ClientId client_id, std::shared_ptr<OutputQueue> output_queue)
          : client_id_(client_id), output_queue_(std::move(output_queue)) {
      }
      void on_result(uint64 id, td_api::object_ptr<td_api::Object> result) final {
        output_queue_->writer_put({client_id_, id, std::move(result)});
      }
      void on_error(uint64 id, td_api::object_ptr<td_api::error> error) final {
        output_queue_->writer_put({client_id_, id, std::move(error)});
      }
      Callback(const Callback &) = delete;
      Callback &operator=(const Callback &) = delete;
      Callback(Callback &&) = delete;
      Callback &operator=(Callback &&) = delete;
      ~Callback() final {
        output_queue_->writer_put({client_id_, 0, nullptr});
      }

     private:
      ClientManager::ClientId client_id_;
      std::shared_ptr<OutputQueue> output_queue_;
    };
    return td::make_unique<Callback>(client_id, output_queue_);
  }

  void add_response(ClientManager::ClientId client_id, uint64 id, td_api::object_ptr<td_api::Object> result) {
    output_queue_->writer_put({client_id, id, std::move(result)});
  }

 private:
  using OutputQueue = MpscPollableQueue<ClientManager::Response>;
  std::shared_ptr<OutputQueue> output_queue_;
  int output_queue_ready_cnt_{0};
  std::atomic<bool> receive_lock_{false};

  ClientManager::Response receive_unlocked(double timeout) {
    if (output_queue_ready_cnt_ == 0) {
      output_queue_ready_cnt_ = output_queue_->reader_wait_nonblock();
    }
    if (output_queue_ready_cnt_ > 0) {
      output_queue_ready_cnt_--;
      return output_queue_->reader_get_unsafe();
    }
    if (timeout != 0) {
      output_queue_->reader_get_event_fd().wait(static_cast<int>(timeout * 1000));
      return receive_unlocked(0);
    }
    return {0, 0, nullptr};
  }
};

class MultiImpl {
 public:
  static constexpr int32 ADDITIONAL_THREAD_COUNT = 3;

  explicit MultiImpl(std::shared_ptr<NetQueryStats> net_query_stats) {
    concurrent_scheduler_ = std::make_shared<ConcurrentScheduler>(ADDITIONAL_THREAD_COUNT, 0);
    concurrent_scheduler_->start();

    {
      auto guard = concurrent_scheduler_->get_main_guard();
      Td::Options options;
      options.net_query_stats = std::move(net_query_stats);
      auto global_dispatch = get_global_external_dispatch();
      multi_td_ = create_actor<MultiTd>("MultiTd", std::move(options), std::move(global_dispatch));
    }

    scheduler_thread_ = thread([concurrent_scheduler = concurrent_scheduler_] {
      while (concurrent_scheduler->run_main(10)) {
      }
    });
  }
  MultiImpl(const MultiImpl &) = delete;
  MultiImpl &operator=(const MultiImpl &) = delete;
  MultiImpl(MultiImpl &&) = delete;
  MultiImpl &operator=(MultiImpl &&) = delete;

  static int32 create_id() {
    auto result = current_id_.fetch_add(1);
    CHECK(result <= static_cast<uint32>(std::numeric_limits<int32>::max()));
    return static_cast<int32>(result);
  }

  void create(int32 td_id, unique_ptr<TdCallback> callback) {
    LOG(INFO) << "Initialize client " << td_id;
    auto guard = concurrent_scheduler_->get_send_guard();
    send_closure(multi_td_, &MultiTd::create, td_id, std::move(callback));
  }

  static bool is_valid_client_id(int32 client_id) {
    return client_id > 0 && static_cast<uint32>(client_id) < current_id_.load();
  }

  void send(ClientManager::ClientId client_id, ClientManager::RequestId request_id,
            td_api::object_ptr<td_api::Function> &&request) {
    auto guard = concurrent_scheduler_->get_send_guard();
    send_closure(multi_td_, &MultiTd::send, client_id, request_id, std::move(request));
  }

  void close(ClientManager::ClientId client_id) {
    LOG(INFO) << "Close client";
    auto guard = concurrent_scheduler_->get_send_guard();
    send_closure(multi_td_, &MultiTd::close, client_id);
  }

  void complete_query(int32 client_id, uint64 query_id, ExternalQueryResult result) {
    auto guard = concurrent_scheduler_->get_send_guard();
    send_closure(multi_td_, &MultiTd::complete_external, client_id, query_id, std::move(result));
  }

  void push_updates(int32 client_id, std::string updates_bytes) {
    auto guard = concurrent_scheduler_->get_send_guard();
    send_closure(multi_td_, &MultiTd::push_updates, client_id, std::move(updates_bytes));
  }

  static void register_client(int32 client_id, std::shared_ptr<MultiImpl> impl) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    clients_[client_id] = std::move(impl);
  }

  static void unregister_client(int32 client_id) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    clients_.erase(client_id);
  }

  static std::shared_ptr<MultiImpl> get_client_impl(int32 client_id) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
      return nullptr;
    }
    return it->second.lock();
  }

  ~MultiImpl() {
    {
      auto guard = concurrent_scheduler_->get_send_guard();
      multi_td_.reset();
      Scheduler::instance()->finish();
    }
    if (!ExitGuard::is_exited()) {
      scheduler_thread_.join();
    } else {
      scheduler_thread_.detach();
    }
    concurrent_scheduler_->finish();
  }

 private:
  std::shared_ptr<ConcurrentScheduler> concurrent_scheduler_;
  thread scheduler_thread_;
  ActorOwn<MultiTd> multi_td_;

  static std::atomic<uint32> current_id_;
  static std::mutex clients_mutex_;
  static FlatHashMap<int32, std::weak_ptr<MultiImpl>> clients_;
};

std::atomic<uint32> MultiImpl::current_id_{1};
std::mutex MultiImpl::clients_mutex_;
FlatHashMap<int32, std::weak_ptr<MultiImpl>> MultiImpl::clients_;

class MultiImplPool {
 public:
  std::shared_ptr<MultiImpl> get() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (impls_.empty()) {
      init_openssl_threads();

      auto max_client_threads = clamp(thread::hardware_concurrency(), 8u, 20u) * 5 / 4;
#if TD_OPENBSD
      max_client_threads = td::min(max_client_threads, 4u);
#endif
      impls_.resize(max_client_threads);
      CHECK(impls_.size() * (1 + MultiImpl::ADDITIONAL_THREAD_COUNT + 1 /* IOCP */) < 128);

      net_query_stats_ = std::make_shared<NetQueryStats>();
    }
    auto &impl = *std::min_element(impls_.begin(), impls_.end(),
                                   [](auto &a, auto &b) { return a.lock().use_count() < b.lock().use_count(); });
    auto result = impl.lock();
    if (!result) {
      result = std::make_shared<MultiImpl>(net_query_stats_);
      impl = result;
    }
    return result;
  }

  void try_clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (impls_.empty()) {
      return;
    }

    for (auto &impl : impls_) {
      if (impl.lock().use_count() != 0) {
        return;
      }
    }
    reset_to_empty(impls_);

    CHECK(net_query_stats_.use_count() == 1);
    CHECK(net_query_stats_->get_count() == 0);
    net_query_stats_ = nullptr;
  }

 private:
  std::mutex mutex_;
  std::vector<std::weak_ptr<MultiImpl>> impls_;
  std::shared_ptr<NetQueryStats> net_query_stats_;
};

class ClientManager::Impl final {
 public:
  ClientId create_client_id() {
    auto client_id = MultiImpl::create_id();
    LOG(INFO) << "Created managed client " << client_id;
    {
      auto lock = impls_mutex_.lock_write();
      impls_[client_id];  // create empty MultiImplInfo
    }
    return client_id;
  }

  void send(ClientId client_id, RequestId request_id, td_api::object_ptr<td_api::Function> &&request) {
    auto lock = impls_mutex_.lock_read();
    if (!MultiImpl::is_valid_client_id(client_id)) {
      receiver_.add_response(client_id, request_id,
                             td_api::make_object<td_api::error>(400, "Invalid TDLib instance specified"));
      return;
    }

    auto it = impls_.find(client_id);
    if (it != impls_.end() && it->second.impl == nullptr) {
      lock.reset();

      auto write_lock = impls_mutex_.lock_write();
      it = impls_.find(client_id);
      if (it != impls_.end() && it->second.impl == nullptr) {
        it->second.impl = pool_.get();
        it->second.impl->create(client_id, receiver_.create_callback(client_id));
        MultiImpl::register_client(client_id, it->second.impl);
      }
      write_lock.reset();

      lock = impls_mutex_.lock_read();
      it = impls_.find(client_id);
    }
    if (it == impls_.end() || it->second.is_closed) {
      receiver_.add_response(client_id, request_id, td_api::make_object<td_api::error>(500, "Request aborted"));
      return;
    }
    it->second.impl->send(client_id, request_id, std::move(request));
  }

  Response receive(double timeout) {
    auto response = receiver_.receive(timeout, true);
    if (response.request_id == 0 && response.object != nullptr &&
        response.object->get_id() == td_api::updateAuthorizationState::ID &&
        static_cast<const td_api::updateAuthorizationState *>(response.object.get())->authorization_state_->get_id() ==
            td_api::authorizationStateClosed::ID) {
      LOG(INFO) << "Release closed client";
      auto lock = impls_mutex_.lock_write();
      close_impl(response.client_id);

      response.client_id = 0;
      response.object = nullptr;
    }
    if (response.object == nullptr && response.client_id != 0 && response.request_id == 0) {
      auto lock = impls_mutex_.lock_write();
      auto it = impls_.find(response.client_id);
      CHECK(it != impls_.end());
      CHECK(it->second.is_closed);
      impls_.erase(it);

      response.object = td_api::make_object<td_api::updateAuthorizationState>(
          td_api::make_object<td_api::authorizationStateClosed>());

      if (impls_.empty()) {
        reset_to_empty(impls_);
        pool_.try_clear();
      }
    }
    return response;
  }

  void close_impl(ClientId client_id) {
    auto it = impls_.find(client_id);
    CHECK(it != impls_.end());
    if (!it->second.is_closed) {
      it->second.is_closed = true;
      MultiImpl::unregister_client(client_id);
      if (it->second.impl == nullptr) {
        receiver_.add_response(client_id, 0, nullptr);
      } else {
        it->second.impl->close(client_id);
      }
    }
  }

  Impl() = default;
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;
  ~Impl() {
    if (ExitGuard::is_exited()) {
      return;
    }
    LOG(INFO) << "Destroy ClientManager";
    for (auto &it : impls_) {
      close_impl(it.first);
    }
    while (!impls_.empty() && !ExitGuard::is_exited()) {
      receive(0.1);
    }
  }

 private:
  MultiImplPool pool_;
  RwMutex impls_mutex_;
  struct MultiImplInfo {
    std::shared_ptr<MultiImpl> impl;
    bool is_closed = false;
  };
  FlatHashMap<ClientId, MultiImplInfo> impls_;
  TdReceiver receiver_;
};

class Client::Impl final {
 public:
  Impl() {
    static MultiImplPool pool;
    multi_impl_ = pool.get();
    td_id_ = MultiImpl::create_id();
    LOG(INFO) << "Create client " << td_id_;
    multi_impl_->create(td_id_, receiver_.create_callback(td_id_));
  }

  void send(Request request) {
    if (request.id == 0 || request.function == nullptr) {
      LOG(ERROR) << "Drop wrong request " << request.id;
      return;
    }

    multi_impl_->send(td_id_, request.id, std::move(request.function));
  }

  Response receive(double timeout) {
    auto response = receiver_.receive(timeout, false);

    Response old_response;
    old_response.id = response.request_id;
    old_response.object = std::move(response.object);
    return old_response;
  }

  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;
  ~Impl() {
    LOG(INFO) << "Destroy Client";
    multi_impl_->close(td_id_);
    while (!ExitGuard::is_exited()) {
      auto response = receiver_.receive(0.1, false);
      if (response.object == nullptr && response.client_id != 0 && response.request_id == 0) {
        break;
      }
    }
  }

 private:
  std::shared_ptr<MultiImpl> multi_impl_;
  TdReceiver receiver_;

  int32 td_id_;
};
#endif

void complete_external_query(int32 client_id, uint64 query_id, ExternalQueryResult result) {
#if TD_THREAD_UNSUPPORTED || TD_EVENTFD_UNSUPPORTED
  auto query = take_external_query(query_id);
  if (query.empty()) {
    return;
  }
  if (result.is_ok) {
    query->set_ok(BufferSlice(Slice(result.ok_data.data(), result.ok_data.size())));
  } else {
    query->set_error(Status::Error(result.error_code, result.error_message));
  }
  NetQueryDispatcher::complete_net_query(std::move(query));
#else
  // Called from outside TDLib's scheduler threads (the embedder's network
  // thread).  Only the id and plain result cross into TDLib via the send
  // guard; the stashed NetQuery is taken and finished on the client's
  // scheduler thread, so no actor is touched or migrated off-thread.
  auto impl = MultiImpl::get_client_impl(client_id);
  if (impl) {
    impl->complete_query(client_id, query_id, std::move(result));
  } else {
    LOG(ERROR) << "No MultiImpl found for client " << client_id << ", dropping query " << query_id;
  }
#endif
}

void push_external_updates(int32 client_id, std::string updates_bytes) {
#if TD_THREAD_UNSUPPORTED || TD_EVENTFD_UNSUPPORTED
  // External-dispatch update injection targets the threaded desktop embedder;
  // the single-threaded build has no dedicated network thread to push from.
  LOG(ERROR) << "push_external_updates is not supported in single-threaded builds";
#else
  // Called from outside TDLib's scheduler threads (the embedder's network
  // thread).  Only plain update bytes cross into TDLib via the send guard; they
  // are parsed and handed to UpdatesManager on the client's scheduler thread.
  auto impl = MultiImpl::get_client_impl(client_id);
  if (impl) {
    impl->push_updates(client_id, std::move(updates_bytes));
  } else {
    LOG(ERROR) << "No MultiImpl found for client " << client_id << ", dropping pushed updates";
  }
#endif
}

Client::Client() : impl_(std::make_unique<Impl>()) {
}

void Client::send(Request &&request) {
  impl_->send(std::move(request));
}

Client::Response Client::receive(double timeout) {
  return impl_->receive(timeout);
}

Client::Response Client::execute(Request &&request) {
  Response response;
  response.id = request.id;
  response.object = Td::static_request(std::move(request.function));
  return response;
}

Client::Client(Client &&) noexcept = default;
Client &Client::operator=(Client &&) noexcept = default;
Client::~Client() = default;

ClientManager::ClientManager() : impl_(std::make_unique<Impl>()) {
}

ClientManager::ClientId ClientManager::create_client_id() {
  return impl_->create_client_id();
}

void ClientManager::send(ClientId client_id, RequestId request_id, td_api::object_ptr<td_api::Function> &&request) {
  impl_->send(client_id, request_id, std::move(request));
}

ClientManager::Response ClientManager::receive(double timeout) {
  return impl_->receive(timeout);
}

td_api::object_ptr<td_api::Object> ClientManager::execute(td_api::object_ptr<td_api::Function> &&request) {
  return Td::static_request(std::move(request));
}

static std::atomic<ClientManager::LogMessageCallbackPtr> log_message_callback;

static void log_message_callback_wrapper(int verbosity_level, CSlice message) {
  auto callback = log_message_callback.load(std::memory_order_relaxed);
  if (callback != nullptr) {
    if (check_utf8(message)) {
      callback(verbosity_level, message.c_str());
    } else {
      size_t pos = 0;
      while (1 <= message[pos] && message[pos] <= 126) {
        pos++;
      }
      CHECK(pos + 1 < message.size());
      auto utf8_message = PSTRING() << message.substr(0, pos)
                                    << url_encode(message.substr(pos, message.size() - pos - 1)) << '\n';
      callback(verbosity_level, utf8_message.c_str());
    }
  }
}

void ClientManager::set_log_message_callback(int max_verbosity_level, LogMessageCallbackPtr callback) {
  if (callback == nullptr) {
    ::td::set_log_message_callback(max_verbosity_level, nullptr);
    log_message_callback = nullptr;
  } else {
    log_message_callback = callback;
    ::td::set_log_message_callback(max_verbosity_level, log_message_callback_wrapper);
  }
}

ClientManager::ClientManager(ClientManager &&) noexcept = default;
ClientManager &ClientManager::operator=(ClientManager &&) noexcept = default;
ClientManager::~ClientManager() = default;

ClientManager *ClientManager::get_manager_singleton() {
  static ClientManager client_manager;
  static ExitGuard exit_guard;
  return &client_manager;
}

}  // namespace td
