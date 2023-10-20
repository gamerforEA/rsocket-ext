package loliland.rsocketext.server

import com.fasterxml.jackson.databind.ObjectMapper
import io.rsocket.kotlin.ConnectionAcceptorContext
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import loliland.rsocketext.common.RSocketHandler
import loliland.rsocketext.common.SetupData
import loliland.rsocketext.common.extensions.readValue
import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.set

// TODO: Буфер сообщений при закрытом сокете и отправка как только соединение восстановится
abstract class RSocketServerHandler<S : SetupData>(mapper: ObjectMapper) : RSocketHandler(mapper) {

    val connections = ConcurrentHashMap<String, RSocketConnection<S>>()

    private val setupDataType = (javaClass.getGenericSuperclass() as ParameterizedType).actualTypeArguments[0]

    final override fun setupConnection(ctx: ConnectionAcceptorContext) {
        val setupData = ctx.config.setupPayload.readValue<S>(setupDataType, mapper) {
            ctx.requester.cancel("Failed to setup connection: $it.")
            return
        }

        val connection = RSocketConnection(ctx.requester, setupData)
        if (onConnectionSetup(connection)) {
            connections[setupData.name] = connection
        } else {
            connection.socket.cancel("Forbidden connection.")
            return
        }

        ctx.requester.coroutineContext.job.invokeOnCompletion {
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
}