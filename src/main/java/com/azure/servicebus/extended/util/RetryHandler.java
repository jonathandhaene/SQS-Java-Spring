package com.azure.servicebus.extended.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Utility class for handling retry logic with exponential backoff and jitter.
 * Provides generic retry capabilities for operations that may fail transiently.
 */
public class RetryHandler {
    private static final Logger logger = LoggerFactory.getLogger(RetryHandler.class);

    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final double backoffMultiplier;
    private final long maxBackoffMillis;

    /**
     * Creates a new RetryHandler with specified retry configuration.
     *
     * @param maxAttempts          maximum number of retry attempts
     * @param initialBackoffMillis initial backoff delay in milliseconds
     * @param backoffMultiplier    multiplier for exponential backoff
     * @param maxBackoffMillis     maximum backoff delay cap in milliseconds
     */
    public RetryHandler(int maxAttempts, long initialBackoffMillis, 
                       double backoffMultiplier, long maxBackoffMillis) {
        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    /**
     * Executes a supplier with retry logic.
     *
     * @param operation the operation to retry
     * @param <T>       the return type of the operation
     * @return the result of the operation
     * @throws RuntimeException if all retry attempts are exhausted
     */
    public <T> T executeWithRetry(Supplier<T> operation) {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                
                if (attempt < maxAttempts) {
                    long delay = calculateBackoffDelay(attempt);
                    logger.warn("Operation failed on attempt {}/{}. Retrying after {} ms. Error: {}", 
                               attempt, maxAttempts, delay, e.getMessage());
                    
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                } else {
                    logger.error("Operation failed after {} attempts", maxAttempts, e);
                }
            }
        }
        
        throw new RuntimeException("Operation failed after " + maxAttempts + " attempts", lastException);
    }

    /**
     * Executes a runnable with retry logic.
     *
     * @param operation the operation to retry
     * @throws RuntimeException if all retry attempts are exhausted
     */
    public void executeWithRetry(Runnable operation) {
        executeWithRetry(() -> {
            operation.run();
            return null;
        });
    }

    /**
     * Calculates the backoff delay for a given attempt using exponential backoff with jitter.
     *
     * @param attempt the current attempt number (1-indexed)
     * @return the delay in milliseconds
     */
    private long calculateBackoffDelay(int attempt) {
        // Calculate exponential backoff: initialBackoff * (multiplier ^ (attempt - 1))
        double exponentialDelay = initialBackoffMillis * Math.pow(backoffMultiplier, attempt - 1);
        
        // Cap at max backoff
        long baseDelay = Math.min((long) exponentialDelay, maxBackoffMillis);
        
        // Add jitter (±25% randomness) to prevent thundering herd
        double jitterFactor = 0.75 + (ThreadLocalRandom.current().nextDouble() * 0.5); // Range: 0.75 to 1.25
        long delayWithJitter = (long) (baseDelay * jitterFactor);
        
        return Math.max(delayWithJitter, 0);
    }
}
