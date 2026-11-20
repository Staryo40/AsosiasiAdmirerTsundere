package com.staryo.enums

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
enum class PayloadFault {
    @SerialName("none") NONE,
    @SerialName("truncated") TRUNCATED,
    @SerialName("deep_nesting") DEEP_NESTING,
    @SerialName("invalid_utf8") INVALID_UTF8,
    @SerialName("oversized") OVERSIZED,
}
