package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.exception.SilentCancellationException
import loliland.rsocketext.common.extensions.jsonPayload
import kotlin.concurrent.Volatile
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

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
        timeout: Duration = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed metadataPush: connection is closed.")
        }
    ) {
        waitConnection(timeout)?.metadataPush(metadata) ?: ifConnectionClosed()
    }

    suspend fun fireAndForget(
        payload: Payload,
        timeout: Duration = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed fireAndForget: connection is closed.")
        }
    ) {
        waitConnection(timeout)?.fireAndForget(payload) ?: ifConnectionClosed()
    }

    suspend fun requestResponse(payload: Payload, timeout: Duration = DEFAULT_TIMEOUT): Payload? {
        return waitConnection(timeout)?.requestResponse(payload)
    }

    suspend fun requestStream(payload: Payload, timeout: Duration = DEFAULT_TIMEOUT): Flow<Payload>? {
        return waitConnection(timeout)?.requestStream(payload)
    }

    suspend fun requestChannel(
        initPayload: Payload,
        payloads: Flow<Payload>,
        timeout: Duration = DEFAULT_TIMEOUT
    ): Flow<Payload>? {
        return waitConnection(timeout)?.requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection(timeout: Duration): RSocket? {
        // Fast path
        getActiveSocket()?.let { return@waitConnection it }

        return withTimeoutOrNull(timeout) {
            while (isActive) {
                getActiveSocket()?.let { return@withTimeoutOrNull socket }
                connectionState.await()
            }

            return@withTimeoutOrNull null
        }
    }

    private fun getActiveSocket(): RSocket? {
        val socket = socket
        return if (socket?.isActive == true) socket else null
    }

    companion object {
        private val DEFAULT_TIMEOUT = 5.minutes
    }
}

fun <T : RSocketClientHandler> T.buildJsonPayload(route: String? = null, data: Any? = null): Payload =
    jsonPayload(route = route, customMetadata = metadata, data = data, mapper = mapper)