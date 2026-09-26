package com.staryo

import com.staryo.state.AppState
import com.staryo.state.DelayRange
import io.ktor.server.config.*
import io.ktor.server.testing.*
import kotlin.test.*

class DelayStateTest {

    @Test
    fun `a valid range is accepted including zero and equal bounds`() {
        DelayRange(500, 3000)
        DelayRange(0, 0)
        DelayRange(3000, 3000)
    }

    @Test
    fun `a negative minimum is rejected`() {
        assertFailsWith<IllegalArgumentException> { DelayRange(-1, 100) }
    }

    @Test
    fun `a minimum above the maximum is rejected and the message names both values`() {
        val error = assertFailsWith<IllegalArgumentException> { DelayRange(3000, 500) }

        assertTrue("3000" in error.message!! && "500" in error.message!!)
    }

    @Test
    fun `state starts with the given delay and no delay by default`() {
        assertEquals(DelayRange(500, 3000), AppState(DelayRange(500, 3000)).responseDelay)
        assertEquals(DelayRange.NONE, AppState().responseDelay)
    }

    @Test
    fun `updating the delay stores it and bumps the change counter`() {
        val state = AppState()
        val before = state.changes.value

        state.updateResponseDelay(DelayRange(1, 2))

        assertEquals(DelayRange(1, 2), state.responseDelay)
        assertEquals(before + 1, state.changes.value)
    }

    @Test
    fun `switching the outage on bumps the counter but switching it off does not`() {
        val state = AppState()
        val start = state.changes.value

        state.outage = true
        assertEquals(start + 1, state.changes.value)

        state.outage = false
        assertEquals(start + 1, state.changes.value)
        assertFalse(state.outage)
    }

    @Test
    fun `changing the schema version does not bump the counter`() {
        val state = AppState()
        val start = state.changes.value

        state.schemaVersion = 2

        assertEquals(start, state.changes.value)
    }

    @Test
    fun `the app refuses to start when the configured minimum is above the maximum`() {
        val error = assertFails {
            testApplication {
                environment {
                    config = ApplicationConfig("application.yaml").mergeWith(
                        MapApplicationConfig("pvmbg.delay.min-ms" to "3000", "pvmbg.delay.max-ms" to "500")
                    )
                }
                startApplication()
            }
        }

        val message = generateSequence<Throwable>(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
        assertTrue("PVMBG_RESPONSE_DELAY_MIN_MS" in message, message)
    }
}
