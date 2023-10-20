package loliland.rsocketext.common

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.core.*
import io.rsocket.kotlin.ConnectionAcceptorContext
import io.rsocket.kotlin.RSocket
import io.rsocket.kotlin.RSocketRequestHandler
import io.rsocket.kotlin.payload.Payload
import kotlinx.coroutines.flow.Flow
import loliland.rsocketext.common.dto.ResponseError
import loliland.rsocketext.common.extensions.errorPayload
import loliland.rsocketext.common.extensions.jsonPayload
import loliland.rsocketext.common.extensions.readJson
import loliland.rsocketext.common.extensions.route
import java.lang.reflect.InvocationTargetException
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.functions
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.jvm.javaType
import kotlin.reflect.typeOf

abstract class RSocketHandler(protected val mapper: ObjectMapper) {

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
    }

    open suspend fun onFireAndForget(request: Payload) {
        try {
            val handler = findHandler(request.routeOrFailed())
            val requestData = handler.decodeRequestData(request)
            val args = if (requestData != null) arrayOf(this, requestData) else arrayOf(this)
            handler.callSuspend(*args)
        } catch (e: Throwable) {
            e.printStackTrace()
        } finally {
            request.close()
        }
    }

    open suspend fun onRequestResponse(request: Payload): Payload {
        return try {
            val route = request.routeOrFailed()
            val handler = findHandler(route)
            val requestData = handler.decodeRequestData(request)
            val args = if (requestData != null) arrayOf(this, requestData) else arrayOf(this)
            when (val response = handler.callSuspend(*args)) {
                is Unit -> Payload.Empty
                is Payload -> response
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

    private fun Payload.routeOrFailed(): String = route() ?: error("No route specified.")

    private fun findHandler(route: String): KFunction<*> {
        return routeHandlers.firstOrNull { it.second.value == route }?.first ?: error("Route $route doesn't exists.")
    }

    private fun KFunction<*>.decodeRequestData(request: Payload): Any? {
        val parameter = parameters.drop(1).singleOrNull() ?: return null
        return if (parameter == typeOf<Payload>()) {
            request
        } else {
            request.data.readJson<Any>(mapper, parameter.type.javaType)
        }
    }

    private inline fun <reified A : Annotation> findHandlers(): List<Pair<KFunction<*>, A>> =
        this::class.functions.filter { it.hasAnnotation<A>() }.map { it to it.findAnnotation<A>()!! }
}