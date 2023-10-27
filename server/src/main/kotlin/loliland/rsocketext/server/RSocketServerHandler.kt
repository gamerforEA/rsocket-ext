package loliland.rsocketext.server

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
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.SetupData
import loliland.rsocketext.common.extensions.readValue
import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.set

abstract class RSocketServerHandler<S : SetupData>(mapper: ObjectMapper) : RSocketHandler(mapper) {

    val connections = ConcurrentHashMap<String, RSocketConnection<S>>()
    private val connectionsStates = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

    private val setupDataType = (javaClass.getGenericSuperclass() as ParameterizedType).actualTypeArguments[0]

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        val setupData = ctx.config.setupPayload.readValue<S>(setupDataType, mapper) {
            ctx.requester.cancel("Failed to setup connection: $it.")
            return
        }

        val connection = RSocketConnection(ctx.requester, setupData)
        if (onConnectionSetup(connection)) {
            connections[setupData.name] = connection
            connectionsStates[setupData.name]?.complete(Unit)
        } else {
            connection.socket.cancel("Forbidden connection.")
            return
        }

        ctx.requester.coroutineContext.job.invokeOnCompletion {
            connectionsStates[setupData.name] = CompletableDeferred()
            connections -= setupData.name
            onConnectionClosed(connection, it)
        }
    }

    abstract fun onConnectionSetup(connection: RSocketConnection<S>): Boolean

    abstract fun onConnectionClosed(connection: RSocketConnection<S>, throwable: Throwable?)

    fun shutdown() {
        eachConnection {
            close(it)
        }
    }

    fun close(connection: RSocketConnection<S>) {
        connection.socket.cancel("Gracefully closed.")
    }

    inline fun eachConnection(action: (RSocketConnection<S>) -> Unit) = connections.values.forEach(action)

    suspend fun metadataPush(connectionName: String, metadata: ByteReadPacket) {
        waitConnection(connectionName).metadataPush(metadata)
    }

    suspend fun fireAndForget(connectionName: String, payload: Payload) {
        waitConnection(connectionName).fireAndForget(payload)
    }

    suspend fun requestResponse(connectionName: String, payload: Payload): Payload {
        return waitConnection(connectionName).requestResponse(payload)
    }

    suspend fun requestStream(connectionName: String, payload: Payload): Flow<Payload> {
        return waitConnection(connectionName).requestStream(payload)
    }

    suspend fun requestChannel(connectionName: String, initPayload: Payload, payloads: Flow<Payload>): Flow<Payload> {
        return waitConnection(connectionName).requestChannel(initPayload, payloads)
    }

    private suspend fun waitConnection(connectionName: String): RSocket {
        while (true) {
            val socket = connections[connectionName]?.socket
            if (socket?.isActive == true) {
                return socket
            }
            val state = connectionsStates[connectionName] ?: error("Unknown connection with name: $connectionName")
            state.await()
        }
    }
}