package com.staryo

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.enums.Scope
import com.staryo.plugins.authorized
import com.staryo.plugins.configureAuthentication
import com.staryo.plugins.configureSerialization
import com.staryo.service.AuthService
import com.staryo.state.AppState
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.test.*

class AuthPluginTest {
    private val config = AuthConfig(
        jwtSecret = "plugin-test-signing-key-plugin-test-signing-key",
        reader = ClientCredentials("reader", "reader-secret"),
        admin = ClientCredentials("admin", "admin-secret"),
        issuer = "pvmbg",
        audience = "pvmbg-api",
        tokenTtl = Duration.ofMinutes(5),
        loginRateLimitPerMinute = 5,
    )
    private val service = AuthService(config, AppState())

    private fun readerToken() = service.login("reader", "reader-secret").access_token
    private fun adminToken() = service.login("admin", "admin-secret").access_token

    // Builds a token by hand so individual claims can be made wrong on purpose.
    private fun forge(
        subject: String = "reader",
        scope: String? = "read",
        issuer: String = config.issuer,
        audience: String = config.audience,
        secret: String = config.jwtSecret,
        expiresAt: Instant = Instant.now().plusSeconds(300),
    ): String = JWT.create()
        .withIssuer(issuer)
        .withAudience(audience)
        .withSubject(subject)
        .apply { if (scope != null) withClaim("scope", scope) }
        .withExpiresAt(expiresAt)
        .sign(Algorithm.HMAC256(secret))

    private fun ApplicationTestBuilder.protectedRoutes() {
        application {
            configureSerialization()
            configureAuthentication(config)
            routing {
                authorized(Scope.READ) { get("/read") { call.respondText("read ok") } }
                authorized(Scope.ADMIN) { get("/admin") { call.respondText("admin ok") } }
            }
        }
    }

    @Test
    fun `no token is 401 on every protected route`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read").status)
        assertEquals(HttpStatusCode.Unauthorized, client.get("/admin").status)
    }

    @Test
    fun `401 carries a bearer challenge header`() = testApplication {
        protectedRoutes()
        assertNotNull(client.get("/read").headers[HttpHeaders.WWWAuthenticate])
    }

    @Test
    fun `reader token reaches read routes but is 403 on admin routes`() = testApplication {
        protectedRoutes()
        val token = readerToken()
        assertEquals(HttpStatusCode.OK, client.get("/read") { bearerAuth(token) }.status)
        assertEquals(HttpStatusCode.Forbidden, client.get("/admin") { bearerAuth(token) }.status)
    }

    @Test
    fun `admin token reaches both read and admin routes`() = testApplication {
        protectedRoutes()
        val token = adminToken()
        assertEquals(HttpStatusCode.OK, client.get("/read") { bearerAuth(token) }.status)
        assertEquals(HttpStatusCode.OK, client.get("/admin") { bearerAuth(token) }.status)
    }

    @Test
    fun `expired token is 401`() = testApplication {
        protectedRoutes()
        val expired = forge(expiresAt = Instant.now().minusSeconds(60))
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(expired) }.status)
    }

    @Test
    fun `token signed with another key is 401`() = testApplication {
        protectedRoutes()
        val forged = forge(secret = "another-signing-key-another-signing-key-1")
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forged) }.status)
    }

    @Test
    fun `tampered token is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(readerToken() + "x") }.status)
    }

    @Test
    fun `malformed token is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth("not-a-jwt") }.status)
    }

    @Test
    fun `wrong issuer or audience is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forge(issuer = "bmkg")) }.status)
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forge(audience = "bmkg-api")) }.status)
    }

    @Test
    fun `validly signed token for an unknown client is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forge(subject = "stranger")) }.status)
    }

    @Test
    fun `token with a missing or unknown scope is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forge(scope = null)) }.status)
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(forge(scope = "superuser")) }.status)
    }

    @Test
    fun `reader identity claiming admin scope is 401`() = testApplication {
        protectedRoutes()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/admin") { bearerAuth(forge(subject = "reader", scope = "admin")) }.status)
    }

    @Test
    fun `token that expired is rejected even though the service issued it`() = testApplication {
        protectedRoutes()
        val past = Clock.fixed(Instant.now().minus(Duration.ofMinutes(10)), java.time.ZoneOffset.UTC)
        val oldToken = AuthService(config, AppState(), past).login("reader", "reader-secret").access_token
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { bearerAuth(oldToken) }.status)
    }

    @Test
    fun `another services credentials are not accepted`() = testApplication {
        protectedRoutes()
        val response = client.get("/read") { header("X-BMKG-Key", "some-bmkg-key") }
        assertEquals(HttpStatusCode.Unauthorized, response.status)
        assertEquals(HttpStatusCode.Unauthorized, client.get("/read") { basicAuth("bmkg", "key") }.status)
    }
}
