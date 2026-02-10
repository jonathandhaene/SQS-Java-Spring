package com.azure.servicebus.extended.util;

import java.util.regex.Pattern;

/**
 * Utility class for validating blob key prefixes.
 * This is the Azure equivalent of AWS's prefix validation in S3.
 */
public class BlobKeyPrefixValidator {
    
    /**
     * Maximum allowed length for blob key prefix.
     * Azure blob names can be up to 1024 characters, minus UUID length (36).
     */
    public static final int MAX_PREFIX_LENGTH = 988;
    
    /**
     * Pattern for allowed characters in blob key prefix.
     * Allows: alphanumeric, '.', '/', '_', '-'
     */
    private static final Pattern VALID_PREFIX_PATTERN = Pattern.compile("^[a-zA-Z0-9._/-]*$");
    
    /**
     * Validates a blob key prefix.
     *
     * @param prefix the blob key prefix to validate
     * @throws IllegalArgumentException if the prefix is invalid
     */
    public static void validate(String prefix) {
        if (prefix == null) {
            return; // null is acceptable (treated as empty)
        }
        
        if (prefix.length() > MAX_PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                String.format("Blob key prefix exceeds maximum length of %d characters: %d",
                    MAX_PREFIX_LENGTH, prefix.length())
            );
        }
        
        if (!VALID_PREFIX_PATTERN.matcher(prefix).matches()) {
            throw new IllegalArgumentException(
                "Blob key prefix contains invalid characters. " +
                "Only alphanumeric characters and '.', '/', '_', '-' are allowed."
            );
        }
    }
}
