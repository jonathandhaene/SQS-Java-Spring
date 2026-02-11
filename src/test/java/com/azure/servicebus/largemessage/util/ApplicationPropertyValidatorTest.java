/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.util;

import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationPropertyValidatorTest {

    @Test
    void testValidate_NullProperties_NoException() {
        // Act & Assert — should not throw
        assertDoesNotThrow(() ->
                ApplicationPropertyValidator.validate(null, 9));
    }

    @Test
    void testValidate_EmptyProperties_NoException() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();

        // Act & Assert
        assertDoesNotThrow(() ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testValidate_WithinLimit() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        // Act & Assert
        assertDoesNotThrow(() ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testValidate_ExceedsMaxAllowedProperties() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            properties.put("key" + i, "value" + i);
        }

        // Act & Assert
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 5));
        assertTrue(ex.getMessage().contains("exceeds maximum allowed"));
    }

    @Test
    void testValidate_ReservedPropertyName_ExtendedPayloadSize() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put(LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME, "value");

        // Act & Assert
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 9));
        assertTrue(ex.getMessage().contains("Reserved property name"));
    }

    @Test
    void testValidate_ReservedPropertyName_Legacy() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put(LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME, "value");

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testValidate_ReservedPropertyName_BlobPointerMarker() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "value");

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testValidate_ReservedPropertyName_UserAgent() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, "value");

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testValidate_TotalSizeTooLarge() {
        // Arrange — create properties that exceed 64 KB
        Map<String, Object> properties = new HashMap<>();
        String largeValue = "x".repeat(70000); // > 64 KB
        properties.put("bigkey", largeValue);

        // Act & Assert
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                ApplicationPropertyValidator.validate(properties, 9));
        assertTrue(ex.getMessage().contains("exceeds safe limit"));
    }

    @Test
    void testValidate_NullValueInProperties() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", null);

        // Act & Assert — should not throw even with null values
        assertDoesNotThrow(() ->
                ApplicationPropertyValidator.validate(properties, 9));
    }

    @Test
    void testIsReserved_True() {
        assertTrue(ApplicationPropertyValidator.isReserved(
                LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME));
        assertTrue(ApplicationPropertyValidator.isReserved(
                LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME));
        assertTrue(ApplicationPropertyValidator.isReserved(
                LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
        assertTrue(ApplicationPropertyValidator.isReserved(
                LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT));
    }

    @Test
    void testIsReserved_False() {
        assertFalse(ApplicationPropertyValidator.isReserved("myCustomProperty"));
        assertFalse(ApplicationPropertyValidator.isReserved(""));
        assertFalse(ApplicationPropertyValidator.isReserved(null));
    }
}
