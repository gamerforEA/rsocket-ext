package loliland.rsocketext.common.extensions

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.TypeFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.treeToValue
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ExperimentalMetadataApi
import io.rsocket.kotlin.metadata.RoutingMetadata
import io.rsocket.kotlin.metadata.metadata
import io.rsocket.kotlin.metadata.read
import io.rsocket.kotlin.payload.Payload
import io.rsocket.kotlin.payload.buildPayload
import loliland.rsocketext.common.dto.ResponseError
import java.lang.reflect.Type

@OptIn(ExperimentalMetadataApi::class)
fun Payload(route: String? = null, packet: ByteReadPacket = ByteReadPacket.Empty): Payload =
    buildPayload {
        route?.also {
            metadata(RoutingMetadata(it))
        }
        data(packet)
    }

fun <T> jsonPayload(route: String? = null, data: T, mapper: ObjectMapper): Payload =
    Payload(route, buildJsonPacket(data, mapper))

inline fun <reified T> ByteReadPacket.readJson(mapper: ObjectMapper): T =
    mapper.readValue<T>(readBytes())

inline fun <reified T> ByteReadPacket.readJson(mapper: ObjectMapper, type: Type): T =
    mapper.readValue(readBytes(), TypeFactory.rawClass(type)) as T

fun <T> buildJsonPacket(value: T, mapper: ObjectMapper): ByteReadPacket =
    buildPacket {
        writeFully(mapper.writeValueAsBytes(value))
    }

@OptIn(ExperimentalMetadataApi::class)
fun Payload.route(): String? = metadata?.read(RoutingMetadata)?.tags?.firstOrNull()

fun errorPayload(route: String? = null, error: ResponseError, mapper: ObjectMapper): Payload =
    Payload(route, buildJsonPacket(mapOf("error" to error), mapper))

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