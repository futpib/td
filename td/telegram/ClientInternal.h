#pragma once

#include "td/telegram/Client.h"
#include "td/telegram/net/NetQueryDispatcher.h"

namespace td {

void set_external_dispatch(ExternalDispatchCallback callback);

}  // namespace td
