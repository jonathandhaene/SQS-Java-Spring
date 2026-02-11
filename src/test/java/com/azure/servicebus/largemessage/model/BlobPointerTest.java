/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BlobPointerTest {

    @Test
    void testConstructorAndGetters() {
        // Arrange & Act
        BlobPointer pointer = new BlobPointer("my-container", "my-blob");

        // Assert
        assertEquals("my-container", pointer.getContainerName());
        assertEquals("my-blob", pointer.getBlobName());
    }

    @Test
    void testToJson() {
        // Arrange
        BlobPointer pointer = new BlobPointer("container-1", "blob-1");

        // Act
        String json = pointer.toJson();

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("container-1"));
        assertTrue(json.contains("blob-1"));
        assertTrue(json.contains("containerName"));
        assertTrue(json.contains("blobName"));
    }

    @Test
    void testFromJson() {
        // Arrange
        String json = "{\"containerName\":\"test-container\",\"blobName\":\"test-blob\"}";

        // Act
        BlobPointer pointer = BlobPointer.fromJson(json);

        // Assert
        assertNotNull(pointer);
        assertEquals("test-container", pointer.getContainerName());
        assertEquals("test-blob", pointer.getBlobName());
    }

    @Test
    void testRoundTrip() {
        // Arrange
        BlobPointer original = new BlobPointer("round-trip-container", "round-trip-blob");

        // Act
        String json = original.toJson();
        BlobPointer restored = BlobPointer.fromJson(json);

        // Assert
        assertEquals(original, restored);
        assertEquals(original.getContainerName(), restored.getContainerName());
        assertEquals(original.getBlobName(), restored.getBlobName());
    }

    @Test
    void testFromJson_InvalidJson_Throws() {
        // Arrange
        String invalidJson = "not-valid-json";

        // Act & Assert
        assertThrows(RuntimeException.class, () -> BlobPointer.fromJson(invalidJson));
    }

    @Test
    void testEquals_SameValues() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob");
        BlobPointer pointer2 = new BlobPointer("container", "blob");

        // Assert
        assertEquals(pointer1, pointer2);
        assertEquals(pointer1.hashCode(), pointer2.hashCode());
    }

    @Test
    void testEquals_DifferentValues() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container-a", "blob-a");
        BlobPointer pointer2 = new BlobPointer("container-b", "blob-b");

        // Assert
        assertNotEquals(pointer1, pointer2);
    }

    @Test
    void testEquals_SameObject() {
        // Arrange
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Assert
        assertEquals(pointer, pointer);
    }

    @Test
    void testEquals_Null() {
        // Arrange
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Assert
        assertNotEquals(null, pointer);
    }

    @Test
    void testEquals_DifferentType() {
        // Arrange
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Assert
        assertNotEquals("not a BlobPointer", pointer);
    }

    @Test
    void testEquals_DifferentContainerName() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container-a", "blob");
        BlobPointer pointer2 = new BlobPointer("container-b", "blob");

        // Assert
        assertNotEquals(pointer1, pointer2);
    }

    @Test
    void testEquals_DifferentBlobName() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob-a");
        BlobPointer pointer2 = new BlobPointer("container", "blob-b");

        // Assert
        assertNotEquals(pointer1, pointer2);
    }

    @Test
    void testHashCode_ConsistentWithEquals() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob");
        BlobPointer pointer2 = new BlobPointer("container", "blob");

        // Assert
        assertEquals(pointer1.hashCode(), pointer2.hashCode());
    }

    @Test
    void testToString() {
        // Arrange
        BlobPointer pointer = new BlobPointer("my-container", "my-blob");

        // Act
        String str = pointer.toString();

        // Assert
        assertNotNull(str);
        assertTrue(str.contains("my-container"));
        assertTrue(str.contains("my-blob"));
        assertTrue(str.contains("BlobPointer"));
    }

    @Test
    void testNullValues() {
        // Arrange & Act
        BlobPointer pointer = new BlobPointer(null, null);

        // Assert
        assertNull(pointer.getContainerName());
        assertNull(pointer.getBlobName());
    }
}
