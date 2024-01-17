package loliland.rsocketext.common.exception

import kotlinx.coroutines.CancellationException

class SilentCancellationException(message: String?) : CancellationException(message) {

    override fun fillInStackTrace(): Throwable {
        return this
    }
}