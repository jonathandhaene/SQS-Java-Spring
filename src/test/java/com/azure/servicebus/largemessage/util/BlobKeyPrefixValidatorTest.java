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

class BlobKeyPrefixValidatorTest {

    @Test
    void testValidate_NullPrefix_NoException() {
        // Act & Assert
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate(null));
    }

    @Test
    void testValidate_EmptyPrefix_NoException() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate(""));
    }

    @Test
    void testValidate_ValidPrefix_Alphanumeric() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("tenantId123"));
    }

    @Test
    void testValidate_ValidPrefix_WithSlashes() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("tenant/region/"));
    }

    @Test
    void testValidate_ValidPrefix_WithDots() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("com.azure.servicebus"));
    }

    @Test
    void testValidate_ValidPrefix_WithUnderscores() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("my_prefix_"));
    }

    @Test
    void testValidate_ValidPrefix_WithHyphens() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("my-prefix-"));
    }

    @Test
    void testValidate_ValidPrefix_MixedCharacters() {
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate("tenant-1/region_2/app.v3"));
    }

    @Test
    void testValidate_InvalidPrefix_SpecialCharacters() {
        assertThrows(IllegalArgumentException.class, () ->
                BlobKeyPrefixValidator.validate("invalid!@#$%"));
    }

    @Test
    void testValidate_InvalidPrefix_Spaces() {
        assertThrows(IllegalArgumentException.class, () ->
                BlobKeyPrefixValidator.validate("has spaces"));
    }

    @Test
    void testValidate_InvalidPrefix_Asterisk() {
        assertThrows(IllegalArgumentException.class, () ->
                BlobKeyPrefixValidator.validate("wild*card"));
    }

    @Test
    void testValidate_InvalidPrefix_QuestionMark() {
        assertThrows(IllegalArgumentException.class, () ->
                BlobKeyPrefixValidator.validate("query?param"));
    }

    @Test
    void testValidate_TooLongPrefix() {
        // Arrange
        String longPrefix = "a".repeat(989); // 989 > 988 max

        // Act & Assert
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                BlobKeyPrefixValidator.validate(longPrefix));
        assertTrue(ex.getMessage().contains("exceeds maximum length"));
    }

    @Test
    void testValidate_ExactMaxLengthPrefix() {
        // Arrange
        String maxPrefix = "a".repeat(988);

        // Act & Assert — should not throw
        assertDoesNotThrow(() -> BlobKeyPrefixValidator.validate(maxPrefix));
    }
}
