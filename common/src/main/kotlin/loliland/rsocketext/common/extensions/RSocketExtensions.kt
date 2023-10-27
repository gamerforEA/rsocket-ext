package loliland.rsocketext.common.extensions

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.TypeFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.treeToValue
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ExperimentalMetadataApi
import io.rsocket.kotlin.core.WellKnownMimeType
import io.rsocket.kotlin.metadata.*
import io.rsocket.kotlin.payload.Payload
import io.rsocket.kotlin.payload.buildPayload
import loliland.rsocketext.common.dto.ResponseError
import java.lang.reflect.Type

@OptIn(ExperimentalMetadataApi::class)
fun Payload(
    route: String? = null,
    customMetadata: RawMetadata? = null,
    packet: ByteReadPacket = ByteReadPacket.Empty
): Payload {
    val metadataEntries = buildList(2) {
        route?.also {
            this += RoutingMetadata(it)
        }
        customMetadata?.also {
            this += customMetadata
        }
    }
    return buildPayload {
        if (metadataEntries.isNotEmpty()) {
            metadata(CompositeMetadata(*metadataEntries.toTypedArray()))
        }
        data(packet)
    }
}

@OptIn(ExperimentalMetadataApi::class)
fun jsonPayload(route: String? = null, customMetadata: Any? = null, data: Any?, mapper: ObjectMapper): Payload =
    Payload(
        route = route,
        customMetadata = customMetadata?.let {
            RawMetadata(
                mimeType = WellKnownMimeType.ApplicationJson,
                content = buildJsonPacket(it, mapper)
            )
        },
        packet = buildJsonPacket(data, mapper)
    )

inline fun <reified T> ByteReadPacket.readJson(mapper: ObjectMapper): T =
    mapper.readValue<T>(readBytes())

inline fun <reified T> ByteReadPacket.readJson(mapper: ObjectMapper, type: Type): T =
    mapper.readValue(readBytes(), TypeFactory.rawClass(type)) as T

fun <T> buildJsonPacket(value: T, mapper: ObjectMapper): ByteReadPacket =
    buildPacket {
        writeFully(mapper.writeValueAsBytes(value))
    }

fun errorPayload(
    route: String? = null,
    customMetadata: Any? = null,
    error: ResponseError,
    mapper: ObjectMapper
): Payload = jsonPayload(
    route = route,
    customMetadata = customMetadata,
    data = mapOf("error" to error),
    mapper = mapper
)

inline fun <reified T : Any> Payload.readValue(mapper: ObjectMapper, onError: (ResponseError) -> T): T {
    return readValue(T::class.java, mapper, onError)
}

@Suppress("UNCHECKED_CAST")
inline fun <T : Any> Payload.readValue(type: Type, mapper: ObjectMapper, onError: (ResponseError) -> T): T {
    val json = data.readJson<JsonNode>(mapper)
    if (json.isObject && json.properties().size == 1 && json.has("error")) {
        return onError(mapper.treeToValue<ResponseError>(json["error"]))
    }
    return mapper.readValue(mapper.treeAsTokens(json), TypeFactory.rawClass(type)) as T
}