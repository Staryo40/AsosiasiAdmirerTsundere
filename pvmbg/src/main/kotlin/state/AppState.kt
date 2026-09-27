package com.staryo.state

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update

class AppState(initialDelay: DelayRange = DelayRange.NONE) {
    private val _changes = MutableStateFlow(0L)

    // Goes up whenever a change should release requests that are currently waiting out their delay.
    val changes: StateFlow<Long> = _changes

    @Volatile var schemaVersion: Int = 1

    @Volatile var outage: Boolean = false
        set(value) {
            field = value
            if (value) _changes.update { it + 1 }
        }

    @Volatile var responseDelay: DelayRange = initialDelay
        private set

    // The value is stored before the counter moves, so a released request already sees the new range.
    fun updateResponseDelay(range: DelayRange) {
        responseDelay = range
        _changes.update { it + 1 }
    }
}
