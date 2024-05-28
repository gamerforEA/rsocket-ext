package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.time.withTimeoutOrNull
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.exception.SilentCancellationException
import loliland.rsocketext.common.extensions.jsonPayload
import java.time.Duration
import kotlin.concurrent.Volatile
import kotlin.coroutines.coroutineContext

abstract class RSocketClientHandler(mapper: ObjectMapper) : RSocketHandler(mapper) {

    @Volatile
    private var socket: RSocket? = null

    val connected get() = socket?.isActive == true

    @Volatile
    private var connectionState = CompletableDeferred<Unit>()

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        val socket = ctx.requester

        synchronized(this) {
            this.socket = socket

            onConnectionSetup(ctx)

            this.connectionState.complete(Unit)
        }

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            synchronized(this) {
                if (this.socket == socket) {
                    this.socket = null
                    this.connectionState = CompletableDeferred()
                }

                onConnectionClosed(socket)
            }
        }
    }

    abstract val metadata: Any?

    abstract fun onConnectionSetup(ctx: ConnectionAcceptorContext)

    abstract fun onConnectionClosed(socket: RSocket)

    fun shutdown() {
        socket?.cancel(SilentCancellationException("Gracefully closed."))
    }

    suspend fun metadataPush(
        metadata: ByteReadPacket,
        duration: Duration = Duration.ofMinutes(5),
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed metadataPush: connection is closed.")
        }
    ) {
        waitConnection(duration)
        socket?.metadataPush(metadata) ?: ifConnectionClosed()
    }

    suspend fun fireAndForget(
        payload: Payload,
        duration: Duration = Duration.ofMinutes(5),
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed fireAndForget: connection is closed.")
        }
    ) {
        waitConnection(duration)
        socket?.fireAndForget(payload) ?: ifConnectionClosed()
    }

    suspend fun requestResponse(payload: Payload, duration: Duration = Duration.ofMinutes(5)): Payload? {
        waitConnection(duration)
        return socket?.requestResponse(payload)
    }

    suspend fun requestStream(payload: Payload, duration: Duration = Duration.ofMinutes(5)): Flow<Payload>? {
        waitConnection(duration)
        return socket?.requestStream(payload)
    }

    suspend fun requestChannel(
        initPayload: Payload,
        payloads: Flow<Payload>,
        duration: Duration = Duration.ofMinutes(5)
    ): Flow<Payload>? {
        waitConnection(duration)
        return socket?.requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection(duration: Duration) {
        while (coroutineContext.isActive && !connected) {
            val timeout = withTimeoutOrNull(duration) { connectionState.await() }
            if (timeout == null) {
                break
            }
        }
    }
}

fun <T : RSocketClientHandler> T.buildJsonPayload(route: String? = null, data: Any? = null): Payload =
    jsonPayload(route = route, customMetadata = metadata, data = data, mapper = mapper)