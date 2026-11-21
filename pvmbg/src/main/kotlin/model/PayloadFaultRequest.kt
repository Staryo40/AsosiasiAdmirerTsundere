package com.staryo.model

import com.staryo.enums.PayloadFault
import kotlinx.serialization.Serializable

@Serializable
data class PayloadFaultRequest(val mode: PayloadFault)
