package com.azure.servicebus.extended.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RetryHandler.
 */
class RetryHandlerTest {

    private RetryHandler retryHandler;

    @BeforeEach
    void setUp() {
        retryHandler = new RetryHandler(3, 100L, 2.0, 5000L);
    }

    @Test
    void testExecuteWithRetry_succeedsOnFirstAttempt() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        
        // Act
        String result = retryHandler.executeWithRetry(() -> {
            callCount.incrementAndGet();
            return "Success";
        });

        // Assert
        assertEquals("Success", result);
        assertEquals(1, callCount.get());
    }

    @Test
    void testExecuteWithRetry_succeedsOnRetry() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        
        // Act
        String result = retryHandler.executeWithRetry(() -> {
            int attempt = callCount.incrementAndGet();
            if (attempt < 3) {
                throw new RuntimeException("Transient failure on attempt " + attempt);
            }
            return "Success after retries";
        });

        // Assert
        assertEquals("Success after retries", result);
        assertEquals(3, callCount.get());
    }

    @Test
    void testExecuteWithRetry_failsAfterMaxRetries() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        
        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            retryHandler.executeWithRetry(() -> {
                callCount.incrementAndGet();
                throw new RuntimeException("Persistent failure");
            });
        });

        assertTrue(exception.getMessage().contains("Operation failed after 3 attempts"));
        assertEquals(3, callCount.get());
    }

    @Test
    void testExecuteWithRetry_runnable_succeedsOnRetry() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        
        // Act
        retryHandler.executeWithRetry(() -> {
            int attempt = callCount.incrementAndGet();
            if (attempt < 2) {
                throw new RuntimeException("Transient failure");
            }
        });

        // Assert
        assertEquals(2, callCount.get());
    }

    @Test
    void testExecuteWithRetry_runnable_failsAfterMaxRetries() {
        // Arrange
        AtomicInteger callCount = new AtomicInteger(0);
        
        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            retryHandler.executeWithRetry(() -> {
                callCount.incrementAndGet();
                throw new RuntimeException("Persistent failure");
            });
        });

        assertTrue(exception.getMessage().contains("Operation failed after 3 attempts"));
        assertEquals(3, callCount.get());
    }

    @Test
    void testRetryHandler_exponentialBackoff() {
        // Test that the retry handler implements exponential backoff
        // This test verifies timing but allows some tolerance
        AtomicInteger callCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        
        try {
            retryHandler.executeWithRetry(() -> {
                callCount.incrementAndGet();
                throw new RuntimeException("Always fail");
            });
        } catch (RuntimeException e) {
            // Expected to fail
        }
        
        long duration = System.currentTimeMillis() - startTime;
        
        // With backoff of 100ms, 200ms (with jitter), should take at least ~200ms
        // but we'll be lenient with timing
        assertTrue(duration >= 150, "Should have some delay from backoff");
        assertEquals(3, callCount.get());
    }

    @Test
    void testRetryHandler_preservesOriginalException() {
        // Arrange
        RuntimeException originalException = new RuntimeException("Original error message");
        
        // Act & Assert
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            retryHandler.executeWithRetry(() -> {
                throw originalException;
            });
        });

        assertTrue(thrown.getMessage().contains("Operation failed after 3 attempts"));
        assertSame(originalException, thrown.getCause());
    }

    @Test
    void testRetryHandler_differentExceptionTypes() {
        // Test that retry works with different exception types
        AtomicInteger callCount = new AtomicInteger(0);
        
        assertThrows(RuntimeException.class, () -> {
            retryHandler.executeWithRetry(() -> {
                int attempt = callCount.incrementAndGet();
                if (attempt == 1) {
                    throw new IllegalArgumentException("First failure");
                } else if (attempt == 2) {
                    throw new IllegalStateException("Second failure");
                } else {
                    throw new RuntimeException("Final failure");
                }
            });
        });

        assertEquals(3, callCount.get());
    }
}
