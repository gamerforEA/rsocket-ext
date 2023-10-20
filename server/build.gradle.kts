@file:Suppress("PropertyName")

val ktor_version: String by project
val rsocket_version: String by project

dependencies {
    implementation(project(":common"))
    implementation("io.ktor:ktor-server-core-jvm:$ktor_version")
    implementation("io.rsocket.kotlin:rsocket-ktor-server:$rsocket_version")
}