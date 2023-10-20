package loliland.rsocketext.server

import io.rsocket.kotlin.RSocket

data class RSocketConnection<S>(val socket: RSocket, val setupData: S)