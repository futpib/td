#pragma once

#include "td/telegram/Client.h"
#include "td/telegram/net/NetQueryDispatcher.h"

#include <string>

namespace td {

// An outbound network query handed to the embedder as plain data.  The owning
// NetQuery never leaves TDLib's scheduler threads: it is stashed inside TDLib
// keyed by `id`, and the embedder gets only the serialized request bytes plus
// routing info.  This keeps actor-owned objects off the embedder's thread, so
// completing a query never migrates an actor across schedulers from outside.
struct ExternalQuery {
  uint64 id = 0;
  std::string query;     // serialized telegram_api function bytes
  bool gzip = false;     // whether `query` should be gzip_packed
  int32 raw_dc_id = 0;   // 0 == main DC
  int32 type = 0;        // NetQuery::Type
};

using GlobalExternalDispatchCallback = std::function<void(int32 client_id, ExternalQuery query)>;

void set_external_dispatch(GlobalExternalDispatchCallback callback);

// Result of an externally-dispatched query, carried as plain data so it can
// cross thread boundaries safely.
struct ExternalQueryResult {
  bool is_ok = false;
  std::string ok_data;
  int32 error_code = 0;
  std::string error_message;
};

// Finish an externally-dispatched query by id.  Safe to call from any thread
// (e.g. the embedder's network thread): only plain data crosses into TDLib; the
// stashed NetQuery is looked up, finished and delivered on a scheduler thread,
// so no actor is ever touched or migrated off-thread.
void complete_external_query(int32 client_id, uint64 query_id, ExternalQueryResult result);

// Inject server-pushed updates that the embedder received on its own connection
// into TDLib's update pipeline.  `updates_bytes` holds a serialized telegram_api
// Updates constructor -- the same bytes a Session would read off the wire.  This
// is the inbound counterpart to set_external_dispatch: external_dispatch forwards
// TDLib's outgoing queries to the embedder, and this carries the server's pushed
// updates back in.  Without it an external-dispatch client sends queries but
// never sees pushes, so its update state freezes after the initial sync.
//
// Safe to call from any thread (e.g. the embedder's network thread): only plain
// data crosses into TDLib; the bytes are parsed and delivered to UpdatesManager
// on the client's scheduler thread, exactly like a native Session's inbound path.
void push_external_updates(int32 client_id, std::string updates_bytes);

}  // namespace td
