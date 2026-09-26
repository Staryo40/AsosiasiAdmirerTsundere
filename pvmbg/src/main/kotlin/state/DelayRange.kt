package com.staryo.state

// Min and max travel together as one immutable value, so a reader can never see one half updated.
data class DelayRange(val minMs: Long, val maxMs: Long) {
    init {
        require(minMs >= 0) { "minimum delay must not be negative (got $minMs ms)" }
        require(minMs <= maxMs) { "minimum delay ($minMs ms) must not be greater than the maximum ($maxMs ms)" }
    }

    companion object {
        val NONE = DelayRange(0, 0)
    }
}
