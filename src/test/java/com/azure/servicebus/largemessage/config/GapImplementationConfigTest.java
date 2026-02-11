/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.storage.blob.models.CustomerProvidedKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for gap implementations in configuration classes:
 * - EncryptionConfiguration with full CustomerProvidedKey SDK support
 * - LargeMessageClientConfiguration new fields (content-type, auto-cleanup, TTL schedule)
 */
class GapImplementationConfigTest {

    // ========== Gap 4: Encryption with CustomerProvidedKey ==========

    @Nested
    @DisplayName("Gap 4: Encryption with CustomerProvidedKey")
    class EncryptionTests {

        @Test
        @DisplayName("Should create SDK CustomerProvidedKey from Base64 key")
        void testToSdkCustomerProvidedKey() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            // 32-byte key in Base64
            config.setCustomerProvidedKey("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");

            CustomerProvidedKey cpk = config.toSdkCustomerProvidedKey();
            assertNotNull(cpk);
        }

        @Test
        @DisplayName("Should return null when no key is configured")
        void testToSdkCustomerProvidedKey_nullWhenNoKey() {
            EncryptionConfiguration config = new EncryptionConfiguration();

            assertNull(config.toSdkCustomerProvidedKey());
        }

        @Test
        @DisplayName("Should return null for empty key")
        void testToSdkCustomerProvidedKey_nullWhenEmptyKey() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setCustomerProvidedKey("");

            assertNull(config.toSdkCustomerProvidedKey());
        }

        @Test
        @DisplayName("hasCustomerProvidedKey should return true when key is set")
        void testHasCustomerProvidedKey_true() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setCustomerProvidedKey("some-base64-key");

            assertTrue(config.hasCustomerProvidedKey());
        }

        @Test
        @DisplayName("hasCustomerProvidedKey should return false when key is null")
        void testHasCustomerProvidedKey_falseNull() {
            EncryptionConfiguration config = new EncryptionConfiguration();

            assertFalse(config.hasCustomerProvidedKey());
        }

        @Test
        @DisplayName("hasCustomerProvidedKey should return false when key is empty")
        void testHasCustomerProvidedKey_falseEmpty() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setCustomerProvidedKey("");

            assertFalse(config.hasCustomerProvidedKey());
        }

        @Test
        @DisplayName("hasEncryptionScope should return true when scope is set")
        void testHasEncryptionScope_true() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setEncryptionScope("my-scope");

            assertTrue(config.hasEncryptionScope());
        }

        @Test
        @DisplayName("hasEncryptionScope should return false when scope is null")
        void testHasEncryptionScope_falseNull() {
            EncryptionConfiguration config = new EncryptionConfiguration();

            assertFalse(config.hasEncryptionScope());
        }

        @Test
        @DisplayName("hasEncryptionScope should return false when scope is empty")
        void testHasEncryptionScope_falseEmpty() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setEncryptionScope("");

            assertFalse(config.hasEncryptionScope());
        }

        @Test
        @DisplayName("Should support customerProvidedKeySha256")
        void testCustomerProvidedKeySha256() {
            EncryptionConfiguration config = new EncryptionConfiguration();
            config.setCustomerProvidedKeySha256("sha256-hash-value");

            assertEquals("sha256-hash-value", config.getCustomerProvidedKeySha256());
        }
    }

    // ========== Gap 7: Content-Type Metadata ==========

    @Nested
    @DisplayName("Gap 7: Content-Type Metadata Configuration")
    class ContentTypeConfigTests {

        @Test
        @DisplayName("Should have default content type of text/plain")
        void testDefaultContentType() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();

            assertEquals("text/plain; charset=utf-8", config.getDefaultContentType());
        }

        @Test
        @DisplayName("Should allow setting custom content type")
        void testCustomContentType() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
            config.setDefaultContentType("application/json");

            assertEquals("application/json", config.getDefaultContentType());
        }
    }

    // ========== Gap 3: Auto Cleanup Configuration ==========

    @Nested
    @DisplayName("Gap 3: Auto Cleanup Configuration")
    class AutoCleanupConfigTests {

        @Test
        @DisplayName("Should default autoCleanupOnComplete to false")
        void testAutoCleanupDefault() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();

            assertFalse(config.isAutoCleanupOnComplete());
        }

        @Test
        @DisplayName("Should allow enabling autoCleanupOnComplete")
        void testAutoCleanupEnable() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
            config.setAutoCleanupOnComplete(true);

            assertTrue(config.isAutoCleanupOnComplete());
        }
    }

    // ========== Gap 5: Scheduled TTL Cleanup Configuration ==========

    @Nested
    @DisplayName("Gap 5: Scheduled TTL Cleanup Configuration")
    class ScheduledTtlCleanupConfigTests {

        @Test
        @DisplayName("Should default TTL cleanup interval to 0 (disabled)")
        void testTtlCleanupIntervalDefault() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();

            assertEquals(0, config.getTtlCleanupIntervalMinutes());
        }

        @Test
        @DisplayName("Should allow setting TTL cleanup interval")
        void testTtlCleanupIntervalSet() {
            LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
            config.setTtlCleanupIntervalMinutes(30);

            assertEquals(30, config.getTtlCleanupIntervalMinutes());
        }
    }
}
