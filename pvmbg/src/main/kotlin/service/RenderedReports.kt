package com.staryo.service

import com.staryo.enums.PayloadFault
import java.io.OutputStream

// Below the 10,000 depth where Go's decoder rejects the whole body, so the consumer's own depth limit is exercised.
const val FAULT_NESTING_DEPTH = 5_000

// Above the Aggregator's default 32 MiB response limit.
const val FAULT_OVERSIZED_BYTES = 40L * 1024 * 1024

private const val CHUNK_BYTES = 64 * 1024

class RenderedReports(private val body: ByteArray, private val fault: PayloadFault) {

    fun writeTo(out: OutputStream) {
        when (fault) {
            PayloadFault.NONE -> out.write(body)
            PayloadFault.TRUNCATED -> out.write(body, 0, maxOf(1, body.size / 2))
            PayloadFault.DEEP_NESTING -> withLeadingElement(out) {
                repeatBytes(out, '['.code.toByte(), FAULT_NESTING_DEPTH.toLong())
                repeatBytes(out, ']'.code.toByte(), FAULT_NESTING_DEPTH.toLong())
            }
            PayloadFault.INVALID_UTF8 -> withLeadingElement(out) {
                out.write("""{"report_id":"fault-""".toByteArray())
                // 0xC3 starts a two-byte sequence that 0x28 cannot continue.
                out.write(byteArrayOf(0xC3.toByte(), 0x28))
                out.write("\"}".toByteArray())
            }
            PayloadFault.OVERSIZED -> withLeadingElement(out) {
                out.write("""{"report_id":"fault-oversized","padding":"""".toByteArray())
                repeatBytes(out, 'x'.code.toByte(), FAULT_OVERSIZED_BYTES)
                out.write("\"}".toByteArray())
            }
        }
    }

    // The body is always a JSON array; the faulty element goes first and the real reports follow intact.
    private fun withLeadingElement(out: OutputStream, writeElement: () -> Unit) {
        out.write('['.code)
        writeElement()
        val empty = body.size <= 2
        if (!empty) out.write(','.code)
        out.write(body, 1, body.size - 1)
    }

    private fun repeatBytes(out: OutputStream, value: Byte, count: Long) {
        val chunk = ByteArray(CHUNK_BYTES) { value }
        var remaining = count
        while (remaining > 0) {
            val size = minOf(remaining, CHUNK_BYTES.toLong()).toInt()
            out.write(chunk, 0, size)
            remaining -= size
        }
    }
}
