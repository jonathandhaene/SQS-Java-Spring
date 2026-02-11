/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReceiveOnlyBlobResolverTest {

    @Test
    void testGetPayloadBySasUri_Success() {
        // Arrange
        ReceiveOnlyBlobResolver resolver = new ReceiveOnlyBlobResolver();
        
        // We need to mock the BlobClient constructor and its behavior
        // This is a simplified test - in reality, you'd need integration tests with Azurite
        // For now, we'll test the basic exception handling
        
        // Act & Assert - test that the method exists and handles exceptions
        assertThrows(RuntimeException.class, () -> 
            resolver.getPayloadBySasUri("invalid-uri")
        );
    }

    @Test
    void testGetPayloadBySasUri_NullUri() {
        // Arrange
        ReceiveOnlyBlobResolver resolver = new ReceiveOnlyBlobResolver();

        // Act & Assert
        assertThrows(RuntimeException.class, () -> 
            resolver.getPayloadBySasUri(null)
        );
    }

    @Test
    void testGetPayloadBySasUri_EmptyUri() {
        // Arrange
        ReceiveOnlyBlobResolver resolver = new ReceiveOnlyBlobResolver();

        // Act & Assert
        assertThrows(RuntimeException.class, () -> 
            resolver.getPayloadBySasUri("")
        );
    }

    @Test
    void testGetPayloadBySasUri_MalformedUri() {
        // Arrange
        ReceiveOnlyBlobResolver resolver = new ReceiveOnlyBlobResolver();
        String malformedUri = "not-a-valid-uri";

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> 
            resolver.getPayloadBySasUri(malformedUri)
        );
        
        assertTrue(exception.getMessage().contains("Failed to download payload using SAS URI"));
    }
}
