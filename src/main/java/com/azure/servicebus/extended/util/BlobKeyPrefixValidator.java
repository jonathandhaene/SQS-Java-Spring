package com.azure.servicebus.extended.util;

import java.util.regex.Pattern;

/**
 * Utility class for validating blob key prefixes.
 */
public class BlobKeyPrefixValidator {
    
    private static final int MAX_PREFIX_LENGTH = 988;
    private static final Pattern VALID_PREFIX_PATTERN = Pattern.compile("^[a-zA-Z0-9._/-]*$");

    /**
     * Validates a blob key prefix.
     *
     * @param prefix the blob key prefix to validate
     * @throws IllegalArgumentException if the prefix is invalid
     */
    public static void validate(String prefix) {
        if (prefix == null) {
            return; // null is allowed (will be treated as empty string)
        }

        if (prefix.length() > MAX_PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                String.format("Blob key prefix exceeds maximum length of %d characters: %d", 
                    MAX_PREFIX_LENGTH, prefix.length()));
        }

        if (!VALID_PREFIX_PATTERN.matcher(prefix).matches()) {
            throw new IllegalArgumentException(
                "Blob key prefix contains invalid characters. Only alphanumeric, dots, underscores, slashes, and hyphens are allowed: " + prefix);
        }
    }
}
