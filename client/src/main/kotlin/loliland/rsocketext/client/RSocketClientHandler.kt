package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.extensions.jsonPayload

// TODO: Буфер сообщений при закрытом сокете и отправка как только соединение восстановится
abstract class RSocketClientHandler(mapper: ObjectMapper) : RSocketHandler(mapper) {

    var socket: RSocket? = null
        private set

    val connected get() = socket?.isActive == true

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        socket = ctx.requester
        onConnectionSetup(ctx)

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            socket?.also(::onConnectionClosed)
            socket = null
        }
    }

    abstract val metadata: Any?

    abstract fun onConnectionSetup(ctx: ConnectionAcceptorContext)

    abstract fun onConnectionClosed(socket: RSocket)

    fun shutdown() {
        socket?.cancel("Gracefully closed.")
    }

    suspend fun metadataPush(metadata: ByteReadPacket) {
        connection.metadataPush(metadata)
    }

    suspend fun fireAndForget(payload: Payload) {
        connection.fireAndForget(payload)
    }

    suspend fun requestResponse(payload: Payload): Payload {
        return connection.requestResponse(payload)
    }

    fun requestStream(payload: Payload): Flow<Payload> {
        return connection.requestStream(payload)
    }

    fun requestChannel(initPayload: Payload, payloads: Flow<Payload>): Flow<Payload> {
        return connection.requestChannel(initPayload, payloads)
    }

    private val connection get() = if (connected) socket!! else error("RSocket is closed")
}

fun <T : RSocketClientHandler> T.buildJsonPayload(route: String? = null, data: Any? = null): Payload =
    jsonPayload(route = route, customMetadata = metadata, data = data, mapper = mapper)