package loliland.rsocketext.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.client.*
import io.ktor.client.engine.*
import io.rsocket.kotlin.core.RSocketConnectorBuilder
import io.rsocket.kotlin.core.WellKnownMimeType
import io.rsocket.kotlin.ktor.client.RSocketSupport
import io.rsocket.kotlin.payload.PayloadMimeType
import kotlinx.coroutines.delay
import loliland.rsocketext.common.extensions.jsonPayload

fun RSocketConnectorBuilder.socketHandler(handler: RSocketClientHandler) {
    acceptor {
        handler.setupConnection(this)
        handler.buildSocketHandler()
    }
}

fun RSocketConnectorBuilder.ConnectionConfigBuilder.setupPayload(setupData: Any, mapper: ObjectMapper) {
    setupPayload {
        jsonPayload(data = setupData, mapper = mapper)
    }
}

fun <C : HttpClientEngineConfig> HttpClientConfig<C>.installDefaultRSocket(
    setupData: Any,
    handler: RSocketClientHandler,
    mapper: ObjectMapper,
    reconnectionDelay: Long = 3000L
) {
    install(RSocketSupport) {
        connector {
            if (reconnectionDelay > 0) {
                reconnectable { _, _ ->
                    delay(reconnectionDelay)
                    true
                }
            }
            connectionConfig {
                payloadMimeType = PayloadMimeType(
                    data = WellKnownMimeType.ApplicationJson,
                    metadata = WellKnownMimeType.ApplicationJson
                )
                setupPayload(setupData, mapper)
            }
            socketHandler(handler)
        }
    }
}