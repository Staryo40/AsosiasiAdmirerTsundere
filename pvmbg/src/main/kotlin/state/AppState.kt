package com.staryo.state

class AppState {
    @Volatile var schemaVersion: Int = 1
    @Volatile var outage: Boolean = false
}
