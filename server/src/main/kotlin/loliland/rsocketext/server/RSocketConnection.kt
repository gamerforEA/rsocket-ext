package loliland.rsocketext.server

import io.rsocket.kotlin.RSocket

class RSocketConnection<S>(val socket: RSocket, val setupData: S)