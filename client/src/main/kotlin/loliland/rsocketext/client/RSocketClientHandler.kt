package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeoutOrNull
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.exception.SilentCancellationException
import loliland.rsocketext.common.extensions.jsonPayload
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

abstract class RSocketClientHandler(mapper: ObjectMapper) : RSocketHandler(mapper) {

    private val socket = MutableStateFlow<RSocket?>(null)

    val connected get() = socket.value?.isActive == true

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        val socket = ctx.requester

        synchronized(this) {
            onConnectionSetup(ctx)
            this.socket.value = socket
        }

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            synchronized(this) {
                // this.socket.compareAndSet(socket, null) - было бы полезно для очистки памяти, но придётся
                // лишний раз будить ждущие в waitConnection корутины.
                onConnectionClosed(socket)
            }
        }
    }

    abstract val metadata: Any?

    abstract fun onConnectionSetup(ctx: ConnectionAcceptorContext)

    abstract fun onConnectionClosed(socket: RSocket)

    fun shutdown() {
        socket.value?.cancel(SilentCancellationException("Gracefully closed."))
    }

    suspend fun metadataPush(
        metadata: ByteReadPacket,
        timeout: Duration? = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed metadataPush: connection is closed.")
        }
    ) {
        waitConnection(timeout)?.metadataPush(metadata) ?: ifConnectionClosed()
    }

    suspend fun fireAndForget(
        payload: Payload,
        timeout: Duration? = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed fireAndForget: connection is closed.")
        }
    ) {
        waitConnection(timeout)?.fireAndForget(payload) ?: ifConnectionClosed()
    }

    suspend fun requestResponse(payload: Payload, timeout: Duration? = DEFAULT_TIMEOUT): Payload? {
        return waitConnection(timeout)?.requestResponse(payload)
    }

    suspend fun requestStream(payload: Payload, timeout: Duration? = DEFAULT_TIMEOUT): Flow<Payload>? {
        return waitConnection(timeout)?.requestStream(payload)
    }

    suspend fun requestChannel(
        initPayload: Payload,
        payloads: Flow<Payload>,
        timeout: Duration? = DEFAULT_TIMEOUT
    ): Flow<Payload>? {
        return waitConnection(timeout)?.requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection(timeout: Duration?): RSocket? {
        // Fast path
        socket.value?.also { if (it.isActive) return@waitConnection it }

        return if (timeout == null) null else withTimeoutOrNull(timeout) {
            socket.firstOrNull { it?.isActive == true }
        }
    }

    companion object {
        private val DEFAULT_TIMEOUT = 5.minutes
    }
}

fun <T : RSocketClientHandler> T.buildJsonPayload(route: String? = null, data: Any? = null): Payload =
    jsonPayload(route = route, customMetadata = metadata, data = data, mapper = mapper)