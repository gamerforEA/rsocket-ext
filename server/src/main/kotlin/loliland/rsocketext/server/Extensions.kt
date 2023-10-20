package loliland.rsocketext.server

import io.ktor.server.routing.*
import io.rsocket.kotlin.ktor.server.rSocket
import loliland.rsocketext.common.SetupData

fun <S : SetupData> Route.rSocket(path: String? = null, protocol: String? = null, handler: RSocketServerHandler<S>) {
    rSocket(path, protocol) {
        handler.setupConnection(this)
        handler.buildSocketHandler()
    }
}