@file:Suppress("LocalVariableName")

import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    java
    kotlin("jvm") version "1.9.0"
    `maven-publish`
    `java-library`
}

repositories {
    mavenCentral()
}

allprojects {
    group = "loliland.rsocketext"
    version = "1.0.0"
}

subprojects {
    apply(plugin = "kotlin")
    apply<JavaPlugin>()
    apply<MavenPublishPlugin>()
    apply<JavaLibraryPlugin>()

    val jackson_version: String by project

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation("com.fasterxml.jackson.core:jackson-databind:$jackson_version")
        implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jackson_version")
    }

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(17)
        }
    }

    kotlin {
        jvmToolchain(17)
        compilerOptions {
            languageVersion.set(KotlinVersion.KOTLIN_1_9)
            apiVersion.set(KotlinVersion.KOTLIN_1_9)
        }
    }

    tasks.withType<Jar> {
        archiveBaseName.set("${rootProject.name}-${project.name}")
    }

    publishing {
        publications {
            create<MavenPublication>(project.name) {
                from(components["java"])
            }
        }
    }
}