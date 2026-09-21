package com.staryo

import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.enums.Scope
import com.staryo.plugins.authorized
import com.staryo.plugins.configureAuthentication
import com.staryo.plugins.configureResponseDelay
import com.staryo.plugins.configureSerialization
import com.staryo.plugins.configureStatusPages
import com.staryo.plugins.isAdminPath
import com.staryo.plugins.outageGuarded
import com.staryo.plugins.pickDelayMs
import com.staryo.state.AppState
import com.staryo.state.DelayRange
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import java.time.Duration
import kotlin.random.Random
import kotlin.test.*

// These check real elapsed time, so they only assert lower bounds and generous upper bounds.
class ResponseDelayTest {
    private val authConfig = AuthConfig(
        jwtSecret = "delay-test-signing-key-delay-test-signing-key",
        reader = ClientCredentials("reader", "reader-secret"),
        admin = ClientCredentials("admin", "admin-secret"),
        issuer = "pvmbg",
        audience = "pvmbg-api",
        tokenTtl = Duration.ofMinutes(5),
        loginRateLimitPerMinute = 5,
    )
    private val state = AppState()

    private fun ApplicationTestBuilder.delayedApp() {
        application {
            configureSerialization()
            configureStatusPages()
            configureResponseDelay(state)
            configureAuthentication(authConfig)
            routing {
                get("/fast") { call.respondText("ok") }
                outageGuarded("/guarded", state) { get { call.respondText("guarded ok") } }
                authorized(Scope.READ) { get("/protected") { call.respondText("protected ok") } }
                route("/admin") { get("/ping") { call.respondText("admin ok") } }
            }
        }
    }

    private suspend fun ApplicationTestBuilder.timed(path: String, block: HttpRequestBuilder.() -> Unit = {}): Pair<HttpResponse, Long> {
        val start = System.nanoTime()
        val response = client.get(path, block)
        return response to (System.nanoTime() - start) / 1_000_000
    }

    // The first request also pays for starting the test app, so it must not be the one that is timed.
    private suspend fun ApplicationTestBuilder.warmUp() {
        client.get("/fast")
    }

    private suspend fun realSleep(ms: Long) = withContext(Dispatchers.Default) { delay(ms) }

    @Test
    fun `a request waits at least the minimum delay`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(400, 400))

        val (response, elapsed) = timed("/fast")

        assertEquals(HttpStatusCode.OK, response.status)
        assertTrue(elapsed >= 350, "took only $elapsed ms")
    }

    @Test
    fun `a zero delay adds nothing`() = testApplication {
        delayedApp()
        warmUp()

        val (_, elapsed) = timed("/fast")

        assertTrue(elapsed < 300, "took $elapsed ms")
    }

    @Test
    fun `a bad token and a missing token are still answered slowly`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(400, 400))

        val (badToken, badElapsed) = timed("/protected") { bearerAuth("garbage") }
        val (noToken, noElapsed) = timed("/protected")

        assertEquals(HttpStatusCode.Unauthorized, badToken.status)
        assertEquals(HttpStatusCode.Unauthorized, noToken.status)
        assertTrue(badElapsed >= 350, "bad token took only $badElapsed ms")
        assertTrue(noElapsed >= 350, "missing token took only $noElapsed ms")
    }

    @Test
    fun `admin paths are never delayed`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(3000, 3000))

        val (response, elapsed) = timed("/admin/ping")

        assertEquals(HttpStatusCode.OK, response.status)
        assertTrue(elapsed < 1000, "took $elapsed ms")
    }

    @Test
    fun `nothing is delayed while the outage is on`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(3000, 3000))
        state.outage = true

        val (guarded, guardedElapsed) = timed("/guarded")
        val (fast, fastElapsed) = timed("/fast")

        assertEquals(HttpStatusCode.ServiceUnavailable, guarded.status)
        assertEquals(HttpStatusCode.OK, fast.status)
        assertTrue(guardedElapsed < 1000, "guarded route took $guardedElapsed ms")
        assertTrue(fastElapsed < 1000, "other route took $fastElapsed ms")
    }

    @Test
    fun `a range change releases a waiting request which then answers normally`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(5000, 5000))

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/fast") }
            realSleep(300)
            state.updateResponseDelay(DelayRange.NONE)

            val response = pending.await()
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("ok", response.bodyAsText())
            assertTrue(elapsed < 3000, "was not released, took $elapsed ms")
        }
    }

    @Test
    fun `switching the outage on releases a waiting request with 503`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(5000, 5000))

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/fast") }
            realSleep(300)
            state.outage = true

            val response = pending.await()
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertEquals(HttpStatusCode.ServiceUnavailable, response.status)
            assertTrue(elapsed < 3000, "was not released, took $elapsed ms")
        }
    }

    @Test
    fun `an unrelated change does not release a waiting request`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(900, 900))

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/fast") }
            realSleep(200)
            state.schemaVersion = 2
            state.outage = false

            val response = pending.await()
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertEquals(HttpStatusCode.OK, response.status)
            assertTrue(elapsed >= 800, "was released early, took only $elapsed ms")
        }
    }

    @Test
    fun `many delayed requests wait in parallel without blocking each other`() = testApplication {
        delayedApp()
        warmUp()
        state.updateResponseDelay(DelayRange(500, 500))

        val start = System.nanoTime()
        val responses = coroutineScope { (1..60).map { async { client.get("/fast") } }.awaitAll() }
        val elapsed = (System.nanoTime() - start) / 1_000_000

        assertTrue(responses.all { it.status == HttpStatusCode.OK })
        assertTrue(elapsed < 5000, "60 requests took $elapsed ms, they seem to block each other")
    }

    @Test
    fun `picked delays stay within the range and reach both ends`() {
        val picks = (1..2000).map { pickDelayMs(DelayRange(2, 5), Random(it)) }

        assertTrue(picks.all { it in 2..5 })
        assertTrue(2L in picks && 5L in picks)
    }

    @Test
    fun `equal bounds give a fixed delay and a huge range does not overflow`() {
        assertEquals(3000, pickDelayMs(DelayRange(3000, 3000), Random.Default))
        assertEquals(0, pickDelayMs(DelayRange.NONE, Random.Default))
        val huge = pickDelayMs(DelayRange(Long.MAX_VALUE - 1, Long.MAX_VALUE), Random.Default)
        assertTrue(huge >= Long.MAX_VALUE - 1)
    }

    @Test
    fun `only paths under admin count as admin paths`() {
        assertTrue(isAdminPath("/admin"))
        assertTrue(isAdminPath("/admin/outage"))
        assertTrue(isAdminPath("/admin/"))
        assertFalse(isAdminPath("/administrator"))
        assertFalse(isAdminPath("/"))
        assertFalse(isAdminPath("/health"))
        assertFalse(isAdminPath("/volcanic-reports/admin"))
    }
}
