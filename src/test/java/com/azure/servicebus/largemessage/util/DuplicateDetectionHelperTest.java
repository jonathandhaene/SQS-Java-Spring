/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuplicateDetectionHelperTest {

    @Test
    void testComputeContentHash_NonNull() {
        // Arrange
        String content = "Hello, World!";

        // Act
        String hash = DuplicateDetectionHelper.computeContentHash(content);

        // Assert
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
    }

    @Test
    void testComputeContentHash_NullContent() {
        // Act — null should be treated as empty string
        String hash = DuplicateDetectionHelper.computeContentHash(null);

        // Assert
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
    }

    @Test
    void testComputeContentHash_EmptyContent() {
        // Act
        String hash = DuplicateDetectionHelper.computeContentHash("");

        // Assert
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
    }

    @Test
    void testComputeContentHash_NullAndEmptyProduceSameHash() {
        // Act
        String hashNull = DuplicateDetectionHelper.computeContentHash(null);
        String hashEmpty = DuplicateDetectionHelper.computeContentHash("");

        // Assert — null is treated as "", so hashes should match
        assertEquals(hashNull, hashEmpty);
    }

    @Test
    void testComputeContentHash_SameContentProducesSameHash() {
        // Arrange
        String content = "Same content for both calls";

        // Act
        String hash1 = DuplicateDetectionHelper.computeContentHash(content);
        String hash2 = DuplicateDetectionHelper.computeContentHash(content);

        // Assert
        assertEquals(hash1, hash2);
    }

    @Test
    void testComputeContentHash_DifferentContentProducesDifferentHash() {
        // Arrange
        String content1 = "Content A";
        String content2 = "Content B";

        // Act
        String hash1 = DuplicateDetectionHelper.computeContentHash(content1);
        String hash2 = DuplicateDetectionHelper.computeContentHash(content2);

        // Assert
        assertNotEquals(hash1, hash2);
    }

    @Test
    void testComputeContentHash_IsBase64Encoded() {
        // Arrange
        String content = "Test content";

        // Act
        String hash = DuplicateDetectionHelper.computeContentHash(content);

        // Assert — SHA-256 produces 32 bytes, Base64 encoded = 44 characters
        assertEquals(44, hash.length());
        // Verify it's valid Base64
        assertDoesNotThrow(() -> java.util.Base64.getDecoder().decode(hash));
    }

    @Test
    void testComputeContentHash_LargeContent() {
        // Arrange
        String largeContent = "x".repeat(1_000_000); // 1 MB

        // Act
        String hash = DuplicateDetectionHelper.computeContentHash(largeContent);

        // Assert
        assertNotNull(hash);
        assertEquals(44, hash.length()); // SHA-256 hash is always the same length
    }

    @Test
    void testComputeContentHash_SpecialCharacters() {
        // Arrange
        String content = "Ünïcödé characters: 日本語 Ελληνικά العربية";

        // Act
        String hash = DuplicateDetectionHelper.computeContentHash(content);

        // Assert
        assertNotNull(hash);
        assertFalse(hash.isEmpty());
    }
}
