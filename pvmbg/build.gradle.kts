
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(ktorLibs.plugins.ktor)
}

group = "com.staryo"
version = "1.0.0-SNAPSHOT"

application {
    mainClass = "com.staryo.MainKt"
}

kotlin {
    jvmToolchain(21)
}

// `./gradlew run` reads the git-ignored .env so secrets never live in the repo; real env vars win over it.
val dotEnv: Map<String, String> = file(".env").takeIf { it.exists() }
    ?.readLines()
    ?.map { it.trim() }
    ?.filter { it.isNotEmpty() && !it.startsWith("#") && "=" in it }
    ?.associate { it.substringBefore("=").trim() to it.substringAfter("=").trim() }
    ?: emptyMap()

tasks.named<JavaExec>("run") {
    dotEnv.forEach { (key, value) -> if (System.getenv(key) == null) environment(key, value) }
}

// Throwaway values
tasks.test {
    environment("PVMBG_JWT_SECRET", "test-only-signing-key-test-only-signing-key")
    environment("PVMBG_READER_CLIENT_ID", "test-reader")
    environment("PVMBG_READER_CLIENT_SECRET", "test-reader-secret")
    environment("PVMBG_ADMIN_CLIENT_ID", "test-admin")
    environment("PVMBG_ADMIN_CLIENT_SECRET", "test-admin-secret")
}

dependencies {
    implementation(ktorLibs.server.config.yaml)
    implementation(ktorLibs.server.core)
    implementation(ktorLibs.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.call.logging)
    implementation(libs.ktor.server.auth)
    implementation(ktorLibs.server.auth.jwt)
    implementation(libs.ktor.server.status.pages)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.logback.classic)

    testImplementation(kotlin("test"))
    testImplementation(ktorLibs.server.testHost)
}
