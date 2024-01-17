package loliland.rsocketext.common.exception

import loliland.rsocketext.common.dto.ResponseError

class ResponseException(val error: ResponseError) : RuntimeException(error.message, null, true, false)