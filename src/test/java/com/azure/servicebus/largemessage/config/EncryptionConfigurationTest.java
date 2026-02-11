/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EncryptionConfigurationTest {

    @Test
    void testDefaultValues() {
        // Arrange & Act
        EncryptionConfiguration config = new EncryptionConfiguration();

        // Assert
        assertNull(config.getEncryptionScope());
        assertNull(config.getCustomerProvidedKey());
    }

    @Test
    void testSetEncryptionScope() {
        // Arrange
        EncryptionConfiguration config = new EncryptionConfiguration();

        // Act
        config.setEncryptionScope("my-encryption-scope");

        // Assert
        assertEquals("my-encryption-scope", config.getEncryptionScope());
    }

    @Test
    void testSetCustomerProvidedKey() {
        // Arrange
        EncryptionConfiguration config = new EncryptionConfiguration();

        // Act
        config.setCustomerProvidedKey("base64-encoded-key");

        // Assert
        assertEquals("base64-encoded-key", config.getCustomerProvidedKey());
    }

    @Test
    void testSetBothValues() {
        // Arrange
        EncryptionConfiguration config = new EncryptionConfiguration();

        // Act
        config.setEncryptionScope("scope-123");
        config.setCustomerProvidedKey("key-456");

        // Assert
        assertEquals("scope-123", config.getEncryptionScope());
        assertEquals("key-456", config.getCustomerProvidedKey());
    }

    @Test
    void testSetNullValues() {
        // Arrange
        EncryptionConfiguration config = new EncryptionConfiguration();
        config.setEncryptionScope("some-scope");
        config.setCustomerProvidedKey("some-key");

        // Act
        config.setEncryptionScope(null);
        config.setCustomerProvidedKey(null);

        // Assert
        assertNull(config.getEncryptionScope());
        assertNull(config.getCustomerProvidedKey());
    }

    @Test
    void testSetEmptyValues() {
        // Arrange
        EncryptionConfiguration config = new EncryptionConfiguration();

        // Act
        config.setEncryptionScope("");
        config.setCustomerProvidedKey("");

        // Assert
        assertEquals("", config.getEncryptionScope());
        assertEquals("", config.getCustomerProvidedKey());
    }
}
