@file:Suppress("PropertyName")

val ktor_version: String by project
val rsocket_version: String by project

dependencies {
    api(project(":common"))
    implementation("io.ktor:ktor-client-core-jvm:$ktor_version")
    implementation("io.rsocket.kotlin:rsocket-ktor-client:$rsocket_version")
}