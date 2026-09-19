package com.staryo

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting() {
    monitor.subscribe(ApplicationStarted) {
        val port = environment.config.property("ktor.deployment.port").getString()
        val host = environment.config.propertyOrNull("ktor.deployment.host")?.getString() ?: "localhost"
        println("PVMBG is running on http://$host:$port")
    }
    routing {
        get("/") {
            call.respondText("Hello, World!")
        }
        
        get("/health") {
            
        }
        
        get("/volcanic-reports") {
            
        }
        
        route("/admin") {
            post("/schema-version") {
                
            }
            
            post("/outage") {
                
            }
        }
    }
}