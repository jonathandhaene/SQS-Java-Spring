/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.integration;

import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared base class for all integration tests.
 *
 * <p>Provides helper methods for building test configurations, generating
 * test messages of various sizes, and reading environment variables that
 * supply live Azure credentials when running on Azure.
 */
public abstract class IntegrationTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    /** 1 KB — forces most test payloads to be offloaded. */
    protected static final int TEST_THRESHOLD = 1024;

    // =========================================================================
    // Configuration helpers
    // =========================================================================

    /**
     * Creates a {@link LargeMessageClientConfiguration} tuned for fast,
     * deterministic integration tests.
     */
    protected LargeMessageClientConfiguration createTestConfiguration() {
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
        config.setMessageSizeThreshold(TEST_THRESHOLD);
        config.setPayloadSupportEnabled(true);
        config.setCleanupBlobOnDelete(true);
        config.setTracingEnabled(false);
        config.setEnableDuplicateDetectionId(false);
        config.setUseLegacyReservedAttributeName(false);

        // Fast retries for tests
        config.setRetryMaxAttempts(2);
        config.setRetryBackoffMillis(50);
        config.setRetryBackoffMultiplier(1.5);
        config.setRetryMaxBackoffMillis(200);

        return config;
    }

    // =========================================================================
    // Message generators
    // =========================================================================

    /** Returns a message body smaller than {@link #TEST_THRESHOLD}. */
    protected String generateSmallMessage() {
        return "small-test-message-" + System.nanoTime();
    }

    /** Returns a message body larger than {@link #TEST_THRESHOLD}. */
    protected String generateLargeMessage() {
        return "L".repeat(TEST_THRESHOLD + 512);
    }

    /**
     * Returns a message body of exactly the requested size (in bytes,
     * assuming ASCII / single-byte UTF-8 characters).
     */
    protected String generateMessageOfSize(int sizeBytes) {
        return "X".repeat(sizeBytes);
    }

    // =========================================================================
    // Environment helpers
    // =========================================================================

    /** Reads an env var or falls back to a default. */
    protected static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    /** Reads a required env var; throws if missing. */
    protected static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Required environment variable not set: " + key);
        }
        return value;
    }

    /** Returns {@code true} when the Azure env vars needed for live tests are set. */
    protected static boolean isAzureEnvironmentAvailable() {
        return System.getenv("AZURE_SERVICEBUS_CONNECTION_STRING") != null
                && System.getenv("AZURE_STORAGE_CONNECTION_STRING") != null;
    }
}
