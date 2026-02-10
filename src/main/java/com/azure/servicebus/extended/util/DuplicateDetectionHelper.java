package com.azure.servicebus.extended.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Utility class for duplicate detection ID generation.
 */
public class DuplicateDetectionHelper {
    
    /**
     * Computes a SHA-256 hash of the content for use as a message ID.
     *
     * @param content the content to hash
     * @return the base64-encoded hash
     */
    public static String computeContentHash(String content) {
        if (content == null) {
            content = "";
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}
