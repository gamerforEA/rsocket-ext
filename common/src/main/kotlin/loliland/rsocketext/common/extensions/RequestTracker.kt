package loliland.rsocketext.common.extensions

import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.TimeSource

class RequestTracker {
    // TODO Something like kotlinx.coroutines.internal.LockFreeLinkedListNode
    private val runningRequests = ConcurrentHashMap.newKeySet<RequestImpl>()!!

    fun findLongRunningRequests(duration: Duration): List<Request> = runningRequests.mapNotNull {
        val deadline = it.startTime + duration
        if (deadline.hasPassedNow()) it else null
    }

    suspend fun <R> execute(route: String, block: suspend () -> R): R {
        val request = RequestImpl(route)
        runningRequests.add(request)

        try {
            return block()
        } finally {
            runningRequests.remove(request)
        }
    }

    interface Request {
        val route: String

        val startTime: TimeSource.Monotonic.ValueTimeMark

        val id: String
    }

    // Must not be data class to use JVM's comparison by identity
    private class RequestImpl(override val route: String) : Request {
        override val startTime = TimeSource.Monotonic.markNow()

        override val id: String get() = Integer.toHexString(System.identityHashCode(this))
    }
}