package com.staryo.model

import kotlinx.serialization.Serializable

@Serializable
data class OutageRequest(val enabled: Boolean)