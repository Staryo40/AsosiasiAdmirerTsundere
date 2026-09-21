package com.staryo

import com.staryo.config.AppConfig
import com.staryo.enums.Scope
import com.staryo.model.*
import com.staryo.plugins.authorized
import com.staryo.service.AuthService
import com.staryo.service.VolcanicReportService
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting(config: AppConfig, service: VolcanicReportService, authService: AuthService) {
    monitor.subscribe(ApplicationStarted) {
        println("PVMBG is running on ${config.url}")
    }
    routing {
        get("/") {
            call.respondText("Hello, World!")
        }

        get("/health") {
            service.ensureAvailable()
            call.respondText("OK")
        }

        post("/auth/login") {
            val request = call.receive<LoginRequest>()
            call.respond(authService.login(request.client_id, request.client_secret))
        }

        authorized(Scope.READ) {
            get("/volcanic-reports") {
                val sinceQuery = call.request.queryParameters["since"]
                call.respond(service.getReports(sinceQuery))
            }
        }

        authorized(Scope.ADMIN) {
            route("/admin") {
                post("/schema-version") {
                    val version = call.receive<SchemaVersionRequest>().version
                    service.setSchemaVersion(version)
                    call.respond(HttpStatusCode.NoContent)
                }

                post("/outage") {
                    val enabled = call.receive<OutageRequest>().enabled
                    service.setOutage(enabled)
                    call.respond(HttpStatusCode.NoContent)
                }
            }
        }
    }
}
