#pragma once

#include "td/telegram/Client.h"
#include "td/telegram/net/NetQueryDispatcher.h"

namespace td {

using GlobalExternalDispatchCallback = std::function<void(int32, NetQueryPtr)>;

void set_external_dispatch(GlobalExternalDispatchCallback callback);

// Safely complete a NetQueryPtr from any thread.
// Unlike NetQueryDispatcher::complete_net_query, this can be called
// from outside TDLib's scheduler threads.
// client_id identifies the TDLib client that owns the query.
void complete_external_query(int32 client_id, NetQueryPtr query);

}  // namespace td
