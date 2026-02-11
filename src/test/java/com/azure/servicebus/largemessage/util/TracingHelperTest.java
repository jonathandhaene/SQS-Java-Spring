/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TracingHelper}.
 *
 * <p>OpenTelemetry API is on the classpath (as a project dependency),
 * so the helper initializes with a noop tracer. All methods should
 * execute without throwing exceptions.</p>
 */
@DisplayName("TracingHelper — OTel Available (Noop Tracer)")
class TracingHelperTest {

    @Test
    @DisplayName("isAvailable returns true when OTel API is on classpath")
    void testIsAvailable_returnsTrue() {
        assertTrue(TracingHelper.isAvailable(),
                "OTel API is on the classpath so isAvailable() should return true");
    }

    @Test
    @DisplayName("startSendSpan does not throw")
    void testStartSendSpan_doesNotThrow() {
        Object span = assertDoesNotThrow(() -> TracingHelper.startSendSpan("test-operation"));
        // Span may be null if noop tracer's reflection fails silently
        TracingHelper.endSpan(span);
    }

    @Test
    @DisplayName("endSpan works with a real span")
    void testEndSpan_worksWithRealSpan() {
        Object span = TracingHelper.startSendSpan("test-end");
        assertDoesNotThrow(() -> TracingHelper.endSpan(span));
    }

    @Test
    @DisplayName("endSpan handles null span gracefully")
    void testEndSpan_handlesNullGracefully() {
        assertDoesNotThrow(() -> TracingHelper.endSpan(null));
    }

    @Test
    @DisplayName("endSpan handles arbitrary non-span object gracefully")
    void testEndSpan_handlesArbitraryObjectGracefully() {
        // Not a real span — endSpan should catch the reflection error
        assertDoesNotThrow(() -> TracingHelper.endSpan("not-a-real-span"));
    }

    @Test
    @DisplayName("recordException works with a real span")
    void testRecordException_worksWithRealSpan() {
        Object span = TracingHelper.startSendSpan("test-exception");
        assertDoesNotThrow(() -> TracingHelper.recordException(span, new RuntimeException("test")));
        TracingHelper.endSpan(span);
    }

    @Test
    @DisplayName("recordException handles null span gracefully")
    void testRecordException_handlesNullSpan() {
        assertDoesNotThrow(() -> TracingHelper.recordException(null, new RuntimeException("test")));
    }

    @Test
    @DisplayName("injectTraceContext runs without error on a properties map")
    void testInjectTraceContext_runsWithoutError() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("existingKey", "existingValue");

        assertDoesNotThrow(() -> TracingHelper.injectTraceContext(properties));

        // Existing properties should remain
        assertEquals("existingValue", properties.get("existingKey"));
    }

    @Test
    @DisplayName("injectTraceContext handles null properties gracefully")
    void testInjectTraceContext_handlesNullProperties() {
        assertDoesNotThrow(() -> TracingHelper.injectTraceContext(null));
    }

    @Test
    @DisplayName("extractTraceContext processes traceparent without error")
    void testExtractTraceContext_processesTraceparent() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("traceparent", "00-abcdef1234567890abcdef1234567890-abcdef1234567890-01");

        assertDoesNotThrow(() -> TracingHelper.extractTraceContext(properties));
        assertTrue(properties.containsKey("traceparent"));
    }

    @Test
    @DisplayName("extractTraceContext handles null properties gracefully")
    void testExtractTraceContext_handlesNullProperties() {
        assertDoesNotThrow(() -> TracingHelper.extractTraceContext(null));
    }

    @Test
    @DisplayName("extractTraceContext handles empty properties")
    void testExtractTraceContext_handlesEmptyProperties() {
        assertDoesNotThrow(() -> TracingHelper.extractTraceContext(new HashMap<>()));
    }

    @Test
    @DisplayName("addAttribute works with a real span")
    void testAddAttribute_worksWithRealSpan() {
        Object span = TracingHelper.startSendSpan("test-attr");
        assertDoesNotThrow(() -> TracingHelper.addAttribute(span, "key", "value"));
        TracingHelper.endSpan(span);
    }

    @Test
    @DisplayName("addAttribute handles null span gracefully")
    void testAddAttribute_handlesNullSpan() {
        assertDoesNotThrow(() -> TracingHelper.addAttribute(null, "key", "value"));
    }

    @Test
    @DisplayName("Full tracing lifecycle completes without error")
    void testFullTracingLifecycle() {
        assertDoesNotThrow(() -> {
            Object span = TracingHelper.startSendSpan("ServiceBus.send");
            // span may be null — all methods handle null gracefully

            TracingHelper.addAttribute(span, "messaging.system", "servicebus");

            Map<String, Object> props = new HashMap<>();
            TracingHelper.injectTraceContext(props);
            TracingHelper.extractTraceContext(props);

            TracingHelper.endSpan(span);
        });
    }

    @Test
    @DisplayName("Full tracing error lifecycle completes without error")
    void testFullTracingErrorLifecycle() {
        assertDoesNotThrow(() -> {
            Object span = TracingHelper.startSendSpan("ServiceBus.send");
            // span may be null — recordException and endSpan handle null

            Exception ex = new RuntimeException("simulated error");
            TracingHelper.recordException(span, ex);
            TracingHelper.endSpan(span);
        });
    }
}
