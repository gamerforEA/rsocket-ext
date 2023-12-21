package loliland.rsocketext.common

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.ExperimentalMetadataApi
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.RSocketRequestHandler
import io.rsocket.kotlin.core.WellKnownMimeType
import io.rsocket.kotlin.metadata.*
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.flow.Flow
import loliland.rsocketext.common.dto.ResponseError
import loliland.rsocketext.common.extensions.errorPayload
import loliland.rsocketext.common.extensions.jsonPayload
import loliland.rsocketext.common.extensions.readJson
import java.lang.reflect.InvocationTargetException
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.full.*
import kotlin.reflect.jvm.javaType
import kotlin.reflect.typeOf

@Suppress("DuplicatedCode")
@OptIn(ExperimentalMetadataApi::class)
abstract class RSocketHandler(val mapper: ObjectMapper) {

    private val routeHandlers = findHandlers<RSocketRoute>()
    private val metadataHandlers = findHandlers<RSocketMetadata>()

    init {
        validateRouteHandler()
        validateMetadataHandler()
    }

    abstract fun setupConnection(ctx: ConnectionAcceptorContext)

    fun buildSocketHandler(): RSocket {
        return RSocketRequestHandler {
            metadataPush {
                onMetadataPush(it)
            }
            fireAndForget {
                onFireAndForget(it)
            }
            requestResponse {
                onRequestResponse(it)
            }
            requestStream {
                onRequestStream(it)
            }
            requestChannel { initPayload, payloads ->
                onRequestChannel(initPayload, payloads)
            }
        }
    }

    open suspend fun onMetadataPush(metadata: ByteReadPacket) {
        metadataHandlers.forEach { (handler) ->
            val packet = metadata.copy()
            try {
                handler.callSuspend(this, packet)
            } catch (e: Throwable) {
                e.printStackTrace()
            } finally {
                packet.close()
            }
        }
        metadata.close()
    }

    open suspend fun onFireAndForget(request: Payload) {
        try {
            val metadataPayload = request.readMetadata()
            val handler = findHandler(metadataPayload.route())
            val payload = handler.decodeRequestData(request)
            val metadata = handler.decodeRequestMetadata(request, metadataPayload)

            val thisRef = handler.parameters.first() to this
            val args = listOfNotNull(thisRef, payload, metadata).toMap()
            handler.callSuspendBy(args)
        } catch (e: Throwable) {
            e.printStackTrace()
        } finally {
            request.close()
        }
    }

    open suspend fun onRequestResponse(request: Payload): Payload {
        return try {
            val metadataPayload = request.readMetadata()
            val handler = findHandler(metadataPayload.route())
            val payload = handler.decodeRequestData(request)
            val metadata = handler.decodeRequestMetadata(request, metadataPayload)

            val thisRef = handler.parameters.first() to this
            val args = listOfNotNull(thisRef, payload, metadata).toMap()
            when (val response = handler.callSuspendBy(args)) {
                is Unit -> Payload.Empty
                is Payload -> response
                is ResponseError -> errorPayload(error = response, mapper = mapper)
                else -> jsonPayload(data = response, mapper = mapper)
            }
        } catch (e: Throwable) {
            if (e !is InvocationTargetException) {
                e.printStackTrace()
            }

            val message = e.message ?: "Unknown error"
            errorPayload(error = ResponseError(code = message.hashCode(), message = message), mapper = mapper)
        } finally {
            request.close()
        }
    }

    // TODO: Implement onRequestStream
    open suspend fun onRequestStream(request: Payload): Flow<Payload> {
        request.close()
        throw NotImplementedError("Request Channel is not implemented.")
    }

    // TODO: Implement onRequestChannel
    open suspend fun onRequestChannel(initPayload: Payload, payloads: Flow<Payload>): Flow<Payload> {
        initPayload.close()
        throw NotImplementedError("Request Channel is not implemented.")
    }

    private fun validateRouteHandler() {
        val routes = mutableSetOf<String>()
        routeHandlers.forEach { (handler, route) ->
            if (route.value in routes) {
                throw IllegalStateException("Duplicate @RSocketMetadata handler '${handler.name}' for route: ${route.value}")
            }
            routes += route.value
        }
    }

    private fun validateMetadataHandler() {
        metadataHandlers.forEach { (handler, _) ->
            if (handler.returnType != typeOf<Unit>()) {
                throw IllegalStateException("The @RSocketMetadata handler must have a return Unit type: ${handler.name}")
            }

            val params = handler.parameters
            if (params.size != 1 && params.single().type != typeOf<ByteReadPacket>()) {
                throw IllegalStateException("The @RSocketMetadata handler must have a single param with ByteReadPacket type: ${handler.name}")
            }
        }
    }

    private fun findHandler(route: String): KFunction<*> {
        return routeHandlers.firstOrNull { it.second.value == route }?.first ?: error("Route $route doesn't exists.")
    }

    private fun KFunction<*>.decodeRequestData(request: Payload): Pair<KParameter, Any>? {
        val parameter = parameters.firstOrNull { it.hasAnnotation<RSocketRoute.Payload>() } ?: return null
        return parameter to if (parameter == typeOf<Payload>()) {
            request
        } else {
            request.data.readJson<Any>(mapper, parameter.type.javaType)
        }
    }

    @OptIn(ExperimentalMetadataApi::class)
    private fun KFunction<*>.decodeRequestMetadata(
        request: Payload,
        metadataPayload: CompositeMetadata
    ): Pair<KParameter, Any>? {
        val parameter = parameters.firstOrNull { it.hasAnnotation<RSocketRoute.Metadata>() } ?: return null
        return parameter to if (parameter == typeOf<ByteReadPacket>()) {
            request
        } else {
            val metadata = metadataPayload.getOrNull(WellKnownMimeType.ApplicationJson)
                ?.read(RawMetadata.reader(WellKnownMimeType.ApplicationJson))
            checkNotNull(metadata) {
                "The $name function has a @Metadata parameter, but there is no metadata in the request!"
            }
            metadata.content.readJson<Any>(mapper, parameter.type.javaType)
        }
    }

    private fun Payload.readMetadata(): CompositeMetadata =
        metadata?.read(CompositeMetadata) ?: error("Broken metadata.")

    private fun CompositeMetadata.route(): String =
        getOrNull(WellKnownMimeType.MessageRSocketRouting)?.read(RoutingMetadata)?.tags?.firstOrNull()
            ?: error("No route specified.")

    private inline fun <reified A : Annotation> findHandlers(): List<Pair<KFunction<*>, A>> =
        this::class.functions.filter { it.hasAnnotation<A>() }.map { it to it.findAnnotation<A>()!! }
}