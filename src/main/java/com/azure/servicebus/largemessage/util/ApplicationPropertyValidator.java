/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.util;

import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for validating application properties on Service Bus messages.
 */
public class ApplicationPropertyValidator {
    
    private static final Set<String> RESERVED_PROPERTY_NAMES = new HashSet<>(Arrays.asList(
        LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME,
        LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME,
        LargeMessageClientConfiguration.BLOB_POINTER_MARKER,
        LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT
    ));

    /**
     * Validates application properties.
     *
     * @param properties the properties to validate
     * @param maxAllowedProperties the maximum allowed property count (excluding reserved)
     * @throws IllegalArgumentException if validation fails
     */
    public static void validate(Map<String, Object> properties, int maxAllowedProperties) {
        if (properties == null) {
            return;
        }

        // Count non-reserved properties
        long userPropertyCount = properties.keySet().stream()
            .filter(key -> !RESERVED_PROPERTY_NAMES.contains(key))
            .count();

        if (userPropertyCount > maxAllowedProperties) {
            throw new IllegalArgumentException(
                    "Application properties count (%d) exceeds maximum allowed (%d)".formatted(
                            userPropertyCount, maxAllowedProperties));
        }

        // Check for reserved names in user properties
        for (String key : properties.keySet()) {
            if (RESERVED_PROPERTY_NAMES.contains(key)) {
                throw new IllegalArgumentException(
                    "Reserved property name cannot be used: " + key);
            }
        }

        // Check total size (approximate - Service Bus has a 256KB limit for all message properties)
        int totalSize = 0;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            totalSize += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            if (entry.getValue() != null) {
                totalSize += entry.getValue().toString().getBytes(StandardCharsets.UTF_8).length;
            }
        }

        // Conservative limit: 64KB for properties
        if (totalSize > 65536) {
            throw new IllegalArgumentException(
                    "Total application properties size (%d bytes) exceeds safe limit (64KB)".formatted(totalSize));
        }
    }

    /**
     * Checks if a property name is reserved.
     *
     * @param propertyName the property name to check
     * @return true if the property name is reserved, false otherwise
     */
    public static boolean isReserved(String propertyName) {
        return RESERVED_PROPERTY_NAMES.contains(propertyName);
    }
}
