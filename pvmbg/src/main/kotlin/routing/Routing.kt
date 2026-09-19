package com.staryo

import com.staryo.config.AppConfig
import com.staryo.service.VolcanicReportService
import com.staryo.model.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting(config: AppConfig, service: VolcanicReportService) {
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

        get("/volcanic-reports") {
            val sinceQuery = call.request.queryParameters["since"]
            call.respond(service.getReports(sinceQuery))
        }

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
