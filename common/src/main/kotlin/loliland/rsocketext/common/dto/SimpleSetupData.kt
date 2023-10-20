package loliland.rsocketext.common.dto

import loliland.rsocketext.common.SetupData

data class SimpleSetupData(override val name: String, val token: String) : SetupData