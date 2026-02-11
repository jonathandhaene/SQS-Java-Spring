/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.store.DefaultBlobNameResolver;
import com.azure.servicebus.largemessage.store.DefaultMessageBodyReplacer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LargeMessageClientConfigurationTest {

    private LargeMessageClientConfiguration config;

    @BeforeEach
    void setUp() {
        config = new LargeMessageClientConfiguration();
    }

    // --- Default Values ---

    @Test
    void testDefaultValues() {
        assertEquals(262144, config.getMessageSizeThreshold());
        assertFalse(config.isAlwaysThroughBlob());
        assertTrue(config.isCleanupBlobOnDelete());
        assertEquals("", config.getBlobKeyPrefix());
        assertEquals(3, config.getRetryMaxAttempts());
        assertEquals(1000L, config.getRetryBackoffMillis());
        assertEquals(2.0, config.getRetryBackoffMultiplier());
        assertEquals(30000L, config.getRetryMaxBackoffMillis());
        assertTrue(config.isDeadLetterOnFailure());
        assertEquals("ProcessingFailure", config.getDeadLetterReason());
        assertEquals(10, config.getMaxDeliveryCount());
        assertTrue(config.isPayloadSupportEnabled());
        assertTrue(config.isUseLegacyReservedAttributeName());
        assertFalse(config.isIgnorePayloadNotFound());
        assertFalse(config.isReceiveOnlyMode());
        assertEquals(9, config.getMaxAllowedProperties());
        assertNull(config.getBlobAccessTier());
        assertEquals(0, config.getBlobTtlDays());
        assertFalse(config.isSasEnabled());
        assertEquals(Duration.ofDays(7), config.getSasTokenValidationTime());
        assertEquals("$attachment.sas.uri", config.getMessagePropertyForBlobSasUri());
        assertFalse(config.isEnableDuplicateDetectionId());
        assertTrue(config.isTracingEnabled());
        assertNotNull(config.getEncryption());
    }

    // --- Setters ---

    @Test
    void testSetMessageSizeThreshold() {
        config.setMessageSizeThreshold(512000);
        assertEquals(512000, config.getMessageSizeThreshold());
    }

    @Test
    void testSetAlwaysThroughBlob() {
        config.setAlwaysThroughBlob(true);
        assertTrue(config.isAlwaysThroughBlob());
    }

    @Test
    void testSetCleanupBlobOnDelete() {
        config.setCleanupBlobOnDelete(false);
        assertFalse(config.isCleanupBlobOnDelete());
    }

    @Test
    void testSetBlobKeyPrefix_Valid() {
        config.setBlobKeyPrefix("my-prefix/");
        assertEquals("my-prefix/", config.getBlobKeyPrefix());
    }

    @Test
    void testSetBlobKeyPrefix_Invalid_Throws() {
        assertThrows(IllegalArgumentException.class, () ->
                config.setBlobKeyPrefix("invalid chars!@#"));
    }

    @Test
    void testSetRetryMaxAttempts() {
        config.setRetryMaxAttempts(5);
        assertEquals(5, config.getRetryMaxAttempts());
    }

    @Test
    void testSetRetryBackoffMillis() {
        config.setRetryBackoffMillis(2000L);
        assertEquals(2000L, config.getRetryBackoffMillis());
    }

    @Test
    void testSetRetryBackoffMultiplier() {
        config.setRetryBackoffMultiplier(3.0);
        assertEquals(3.0, config.getRetryBackoffMultiplier());
    }

    @Test
    void testSetRetryMaxBackoffMillis() {
        config.setRetryMaxBackoffMillis(60000L);
        assertEquals(60000L, config.getRetryMaxBackoffMillis());
    }

    @Test
    void testSetDeadLetterOnFailure() {
        config.setDeadLetterOnFailure(false);
        assertFalse(config.isDeadLetterOnFailure());
    }

    @Test
    void testSetDeadLetterReason() {
        config.setDeadLetterReason("CustomReason");
        assertEquals("CustomReason", config.getDeadLetterReason());
    }

    @Test
    void testSetMaxDeliveryCount() {
        config.setMaxDeliveryCount(5);
        assertEquals(5, config.getMaxDeliveryCount());
    }

    @Test
    void testSetPayloadSupportEnabled() {
        config.setPayloadSupportEnabled(false);
        assertFalse(config.isPayloadSupportEnabled());
    }

    @Test
    void testSetIgnorePayloadNotFound() {
        config.setIgnorePayloadNotFound(true);
        assertTrue(config.isIgnorePayloadNotFound());
    }

    @Test
    void testSetReceiveOnlyMode() {
        config.setReceiveOnlyMode(true);
        assertTrue(config.isReceiveOnlyMode());
    }

    @Test
    void testSetMaxAllowedProperties() {
        config.setMaxAllowedProperties(15);
        assertEquals(15, config.getMaxAllowedProperties());
    }

    @Test
    void testSetBlobAccessTier() {
        config.setBlobAccessTier("Cool");
        assertEquals("Cool", config.getBlobAccessTier());
    }

    @Test
    void testSetBlobTtlDays() {
        config.setBlobTtlDays(30);
        assertEquals(30, config.getBlobTtlDays());
    }

    @Test
    void testSetSasEnabled() {
        config.setSasEnabled(true);
        assertTrue(config.isSasEnabled());
    }

    @Test
    void testSetSasTokenValidationTime() {
        config.setSasTokenValidationTime(Duration.ofHours(1));
        assertEquals(Duration.ofHours(1), config.getSasTokenValidationTime());
    }

    @Test
    void testSetMessagePropertyForBlobSasUri() {
        config.setMessagePropertyForBlobSasUri("custom.sas.uri");
        assertEquals("custom.sas.uri", config.getMessagePropertyForBlobSasUri());
    }

    @Test
    void testSetEnableDuplicateDetectionId() {
        config.setEnableDuplicateDetectionId(true);
        assertTrue(config.isEnableDuplicateDetectionId());
    }

    @Test
    void testSetTracingEnabled() {
        config.setTracingEnabled(false);
        assertFalse(config.isTracingEnabled());
    }

    @Test
    void testSetEncryption() {
        EncryptionConfiguration encryption = new EncryptionConfiguration();
        encryption.setEncryptionScope("my-scope");
        config.setEncryption(encryption);
        assertEquals("my-scope", config.getEncryption().getEncryptionScope());
    }

    // --- Reserved Attribute Name ---

    @Test
    void testGetReservedAttributeName_Legacy() {
        config.setUseLegacyReservedAttributeName(true);
        assertEquals(LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME,
                config.getReservedAttributeName());
    }

    @Test
    void testGetReservedAttributeName_Modern() {
        config.setUseLegacyReservedAttributeName(false);
        assertEquals(LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME,
                config.getReservedAttributeName());
    }

    // --- Lazy-Initialized Components ---

    @Test
    void testGetBlobNameResolver_DefaultsToDefaultResolver() {
        var resolver = config.getBlobNameResolver();
        assertNotNull(resolver);
        assertInstanceOf(DefaultBlobNameResolver.class, resolver);
    }

    @Test
    void testGetBodyReplacer_DefaultsToDefaultReplacer() {
        var replacer = config.getBodyReplacer();
        assertNotNull(replacer);
        assertInstanceOf(DefaultMessageBodyReplacer.class, replacer);
    }

    @Test
    void testGetMessageSizeCriteria_DefaultsToDefaultCriteria() {
        var criteria = config.getMessageSizeCriteria();
        assertNotNull(criteria);
        assertInstanceOf(DefaultMessageSizeCriteria.class, criteria);
    }

    @Test
    void testSetBlobNameResolver_Custom() {
        // Arrange
        var customResolver = new DefaultBlobNameResolver("custom-");

        // Act
        config.setBlobNameResolver(customResolver);

        // Assert
        assertSame(customResolver, config.getBlobNameResolver());
    }

    // --- Constants ---

    @Test
    void testConstants() {
        assertEquals(262144, LargeMessageClientConfiguration.DEFAULT_MESSAGE_SIZE_THRESHOLD);
        assertEquals("ExtendedPayloadSize", LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME);
        assertEquals("ServiceBusLargePayloadSize", LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME);
        assertEquals("com.azure.servicebus.largemessage.BlobPointer", LargeMessageClientConfiguration.BLOB_POINTER_MARKER);
        assertEquals("LargeMessageClientUserAgent", LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT);
        assertEquals(9, LargeMessageClientConfiguration.MAX_ALLOWED_PROPERTIES);
    }
}
