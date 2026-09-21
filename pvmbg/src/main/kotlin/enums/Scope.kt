package com.staryo.enums

enum class Scope(val claim: String) {
    READ("read"),
    ADMIN("admin");

    fun includes(required: Scope) = this == ADMIN || this == required

    companion object {
        fun fromClaim(claim: String?) = entries.firstOrNull { it.claim == claim }
    }
}
