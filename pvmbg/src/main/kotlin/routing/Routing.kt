package com.staryo

import com.staryo.config.AppConfig
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting(config: AppConfig) {
    monitor.subscribe(ApplicationStarted) {
        println("PVMBG is running on ${config.url}")
    }
    routing {
        get("/") {
            call.respondText("Hello, World!")
        }
        
        get("/health") {
            
        }
        
        get("/volcanic-reports") {
            // since is a timestamp
            val sinceQuery = call.request.queryParameters["since"]

        }
        
        route("/admin") {
            post("/schema-version") {
                
            }
            
            post("/outage") {
                
            }
        }
    }
}