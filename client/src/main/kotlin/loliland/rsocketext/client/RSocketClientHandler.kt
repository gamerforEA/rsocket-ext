package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.extensions.jsonPayload

abstract class RSocketClientHandler(mapper: ObjectMapper) : RSocketHandler(mapper) {

    private var socket: RSocket? = null

    val connected get() = socket?.isActive == true
    private var connectionState = CompletableDeferred<Unit>()

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        socket = ctx.requester
        onConnectionSetup(ctx)
        connectionState.complete(Unit)

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            connectionState = CompletableDeferred()
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
        waitConnection()
        socket!!.metadataPush(metadata)
    }

    suspend fun fireAndForget(payload: Payload) {
        waitConnection()
        socket!!.fireAndForget(payload)
    }

    suspend fun requestResponse(payload: Payload): Payload {
        waitConnection()
        return socket!!.requestResponse(payload)
    }

    suspend fun requestStream(payload: Payload): Flow<Payload> {
        waitConnection()
        return socket!!.requestStream(payload)
    }

    suspend fun requestChannel(initPayload: Payload, payloads: Flow<Payload>): Flow<Payload> {
        waitConnection()
        return socket!!.requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection() {
        while (!connected) {
            connectionState.await()
        }
    }
}

fun <T : RSocketClientHandler> T.buildJsonPayload(route: String? = null, data: Any? = null): Payload =
    jsonPayload(route = route, customMetadata = metadata, data = data, mapper = mapper)