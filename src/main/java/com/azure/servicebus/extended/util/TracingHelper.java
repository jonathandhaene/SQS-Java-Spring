package com.azure.servicebus.extended.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for optional OpenTelemetry tracing support.
 * This class detects OpenTelemetry on the classpath at runtime and provides tracing capabilities.
 * If OpenTelemetry is not present, all methods are no-ops.
 */
public class TracingHelper {
    
    private static final Logger logger = LoggerFactory.getLogger(TracingHelper.class);
    private static final boolean OTEL_AVAILABLE;
    private static Object tracer = null;

    static {
        boolean available = false;
        try {
            // Try to load OpenTelemetry classes
            Class.forName("io.opentelemetry.api.trace.Tracer");
            Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
            available = true;
            logger.debug("OpenTelemetry detected on classpath - tracing will be available");
            
            // Try to get the tracer
            try {
                Class<?> globalOtelClass = Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
                Object openTelemetry = globalOtelClass.getMethod("get").invoke(null);
                tracer = openTelemetry.getClass()
                    .getMethod("getTracer", String.class)
                    .invoke(openTelemetry, "azure-servicebus-extended-client");
                logger.debug("OpenTelemetry tracer initialized successfully");
            } catch (Exception e) {
                logger.debug("OpenTelemetry classes found but tracer initialization failed: {}", e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            logger.debug("OpenTelemetry not found on classpath - tracing will be disabled");
        }
        OTEL_AVAILABLE = available && tracer != null;
    }

    /**
     * Checks if OpenTelemetry is available on the classpath.
     *
     * @return true if OpenTelemetry is available, false otherwise
     */
    public static boolean isAvailable() {
        return OTEL_AVAILABLE;
    }

    /**
     * Starts a span for a send operation.
     *
     * @param operationName the operation name
     * @return a span object, or null if tracing is not available
     */
    public static Object startSendSpan(String operationName) {
        if (!OTEL_AVAILABLE) {
            return null;
        }

        try {
            // Create a span using reflection to avoid compile-time dependency
            Object span = tracer.getClass()
                .getMethod("spanBuilder", String.class)
                .invoke(tracer, operationName);
            span = span.getClass().getMethod("startSpan").invoke(span);
            return span;
        } catch (Exception e) {
            logger.debug("Failed to start OpenTelemetry span: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Ends a span.
     *
     * @param span the span to end
     */
    public static void endSpan(Object span) {
        if (span == null || !OTEL_AVAILABLE) {
            return;
        }

        try {
            span.getClass().getMethod("end").invoke(span);
        } catch (Exception e) {
            logger.debug("Failed to end OpenTelemetry span: {}", e.getMessage());
        }
    }

    /**
     * Records an exception on a span.
     *
     * @param span the span
     * @param exception the exception to record
     */
    public static void recordException(Object span, Throwable exception) {
        if (span == null || !OTEL_AVAILABLE) {
            return;
        }

        try {
            span.getClass().getMethod("recordException", Throwable.class).invoke(span, exception);
        } catch (Exception e) {
            logger.debug("Failed to record exception on OpenTelemetry span: {}", e.getMessage());
        }
    }

    /**
     * Injects trace context into application properties for propagation.
     *
     * @param properties the application properties map
     */
    public static void injectTraceContext(Map<String, Object> properties) {
        if (!OTEL_AVAILABLE || properties == null) {
            return;
        }

        try {
            // Get current span context and inject it
            Class<?> spanClass = Class.forName("io.opentelemetry.api.trace.Span");
            Object currentSpan = spanClass.getMethod("current").invoke(null);
            Object spanContext = currentSpan.getClass().getMethod("getSpanContext").invoke(currentSpan);
            
            // Get trace ID and span ID
            String traceId = (String) spanContext.getClass().getMethod("getTraceId").invoke(spanContext);
            String spanId = (String) spanContext.getClass().getMethod("getSpanId").invoke(spanContext);
            
            if (traceId != null && !traceId.isEmpty()) {
                // W3C Trace Context format
                properties.put("traceparent", String.format("00-%s-%s-01", traceId, spanId));
            }
        } catch (Exception e) {
            logger.debug("Failed to inject trace context: {}", e.getMessage());
        }
    }

    /**
     * Extracts trace context from application properties.
     *
     * @param properties the application properties map
     */
    public static void extractTraceContext(Map<String, Object> properties) {
        if (!OTEL_AVAILABLE || properties == null) {
            return;
        }

        try {
            String traceparent = (String) properties.get("traceparent");
            if (traceparent != null && !traceparent.isEmpty()) {
                // Parse W3C Trace Context format: version-traceid-spanid-flags
                // For now, we just log that we found it
                logger.debug("Extracted trace context from message: {}", traceparent);
            }
        } catch (Exception e) {
            logger.debug("Failed to extract trace context: {}", e.getMessage());
        }
    }

    /**
     * Adds an attribute to a span.
     *
     * @param span the span
     * @param key the attribute key
     * @param value the attribute value
     */
    public static void addAttribute(Object span, String key, String value) {
        if (span == null || !OTEL_AVAILABLE) {
            return;
        }

        try {
            span.getClass().getMethod("setAttribute", String.class, String.class).invoke(span, key, value);
        } catch (Exception e) {
            logger.debug("Failed to add attribute to OpenTelemetry span: {}", e.getMessage());
        }
    }
}
