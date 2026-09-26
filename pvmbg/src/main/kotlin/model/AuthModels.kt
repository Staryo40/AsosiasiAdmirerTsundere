package com.staryo.model

import kotlinx.serialization.Serializable

@Serializable
data class LoginRequest(val client_id: String, val client_secret: String)

@Serializable
data class TokenResponse(val access_token: String, val token_type: String, val expires_in: Long)
