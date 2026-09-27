package com.staryo.model

import kotlinx.serialization.Serializable

@Serializable
data class DelayRequest(val min_ms: Long, val max_ms: Long)
