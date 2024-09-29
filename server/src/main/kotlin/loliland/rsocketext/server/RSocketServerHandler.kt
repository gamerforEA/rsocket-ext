package loliland.rsocketext.server

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.SetupData
import loliland.rsocketext.common.exception.SilentCancellationException
import loliland.rsocketext.common.extensions.RequestTracker
import loliland.rsocketext.common.extensions.readValue
import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.set
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

abstract class RSocketServerHandler<S : SetupData>(mapper: ObjectMapper, tracker: RequestTracker? = null) :
    RSocketHandler(mapper, tracker) {

    val connections = ConcurrentHashMap<String, RSocketConnection<S>>()
    private val connectionsStates = ConcurrentHashMap<String, ConnectionState>()

    private val setupDataType = (javaClass.getGenericSuperclass() as ParameterizedType).actualTypeArguments[0]

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        val setupData = ctx.config.setupPayload.readValue<S>(setupDataType, mapper) {
            ctx.requester.cancel("Failed to setup connection: $it.")
            return
        }

        val connection = RSocketConnection(ctx.requester, setupData)
        if (!canConnectable(connection)) {
            connection.socket.cancel("Forbidden connection.")
            return
        }

        val connectionName = setupData.name
        val state = connectionsStates.computeIfAbsent(connectionName) { ConnectionState() }

        synchronized(state) {
            connections[connectionName] = connection
            state.onConnect()

            onConnectionSetup(connection)
        }

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            synchronized(state) {
                if (connections.remove(connectionName, connection)) {
                    state.onDisconnect()
                }

                onConnectionClosed(connection, it)
            }
        }
    }

    abstract fun canConnectable(connection: RSocketConnection<S>): Boolean

    abstract fun onConnectionSetup(connection: RSocketConnection<S>)

    abstract fun onConnectionClosed(connection: RSocketConnection<S>, throwable: Throwable?)

    fun shutdown() {
        eachConnection {
            close(it)
        }
    }

    fun close(connection: RSocketConnection<S>) {
        connection.socket.cancel(SilentCancellationException("Gracefully closed."))
    }

    inline fun eachConnection(action: (RSocketConnection<S>) -> Unit) = connections.values.forEach(action)

    suspend fun metadataPush(
        connectionName: String,
        metadata: ByteReadPacket,
        timeout: Duration? = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed metadataPush: connection is closed.")
        }
    ) {
        val connection = waitConnection(connectionName, timeout)
        if (connection != null) {
            connection.metadataPush(metadata)
        } else {
            ifConnectionClosed()
        }
    }

    suspend fun fireAndForget(
        connectionName: String,
        payload: Payload,
        timeout: Duration? = DEFAULT_TIMEOUT,
        ifConnectionClosed: () -> Unit = {
            throw IllegalStateException("Failed fireAndForget: connection is closed.")
        }
    ) {
        val connection = waitConnection(connectionName, timeout)
        if (connection != null) {
            connection.fireAndForget(payload)
        } else {
            ifConnectionClosed()
        }
    }

    suspend fun requestResponse(
        connectionName: String,
        payload: Payload,
        timeout: Duration? = DEFAULT_TIMEOUT
    ): Payload? {
        return waitConnection(connectionName, timeout)?.requestResponse(payload)
    }

    suspend fun requestStream(
        connectionName: String,
        payload: Payload,
        timeout: Duration? = DEFAULT_TIMEOUT
    ): Flow<Payload>? {
        return waitConnection(connectionName, timeout)?.requestStream(payload)
    }

    suspend fun requestChannel(
        connectionName: String,
        initPayload: Payload,
        payloads: Flow<Payload>,
        timeout: Duration? = DEFAULT_TIMEOUT
    ): Flow<Payload>? {
        return waitConnection(connectionName, timeout)?.requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection(connectionName: String, timeout: Duration?): RSocket? {
        // Fast path
        getActiveSocket(connectionName)?.let { return@waitConnection it }

        val state = connectionsStates[connectionName] ?: error("Unknown connection with name: $connectionName")

        if (timeout == null) {
            return null
        }

        val untilDeadline = run {
            val disconnectTime = state.disconnectData.get()?.disconnectTime ?: return null
            val deadline = disconnectTime + timeout
            deadline - TimeSource.Monotonic.markNow()
        }

        if (!untilDeadline.isPositive()) {
            return null
        }

        return withTimeoutOrNull(untilDeadline) {
            while (isActive) {
                getActiveSocket(connectionName)?.let { return@withTimeoutOrNull it }

                val mutex = state.disconnectData.get()?.connectMutex ?: break
                mutex.await()
            }

            return@withTimeoutOrNull null
        }
    }

    private fun getActiveSocket(connectionName: String): RSocket? {
        val socket = connections[connectionName]?.socket
        return if (socket?.isActive == true) socket else null
    }

    private class ConnectionState {
        val disconnectData = AtomicReference<DisconnectData?>()

        fun onConnect() {
            disconnectData.get()?.connectMutex?.complete(Unit)
        }

        fun onDisconnect() = disconnectData.set(DisconnectData())
    }

    private class DisconnectData {
        val disconnectTime = TimeSource.Monotonic.markNow()
        val connectMutex = CompletableDeferred<Unit>()
    }

    companion object {
        private val DEFAULT_TIMEOUT = 5.minutes
    }
}