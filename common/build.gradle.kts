@file:Suppress("PropertyName")

val ktor_version: String by project
val rsocket_version: String by project

dependencies {
    implementation("io.ktor:ktor-utils-jvm:$ktor_version")
    implementation("io.rsocket.kotlin:rsocket-core-jvm:$rsocket_version")
}