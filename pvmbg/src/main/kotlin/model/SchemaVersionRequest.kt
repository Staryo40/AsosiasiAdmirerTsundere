package com.staryo.model

import kotlinx.serialization.Serializable

@Serializable
data class SchemaVersionRequest(val version: Int)