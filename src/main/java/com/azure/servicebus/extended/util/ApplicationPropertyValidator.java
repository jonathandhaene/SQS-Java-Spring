package com.azure.servicebus.extended.util;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for validating Service Bus message application properties.
 * This is the Azure equivalent of AWS's message attribute validation.
 */
public class ApplicationPropertyValidator {
    
    /**
     * Default maximum size for all application properties combined (in bytes).
     * Azure Service Bus has limits on message size and properties.
     */
    public static final int DEFAULT_MAX_PROPERTIES_SIZE = 64 * 1024; // 64 KB
    
    /**
     * Validates application properties before sending a message.
     *
     * @param properties the application properties to validate
     * @param reservedNames set of reserved property names that users cannot set
     * @param maxAllowedProperties maximum number of properties allowed
     * @throws IllegalArgumentException if validation fails
     */
    public static void validate(
            Map<String, Object> properties,
            Set<String> reservedNames,
            int maxAllowedProperties) {
        validate(properties, reservedNames, maxAllowedProperties, DEFAULT_MAX_PROPERTIES_SIZE);
    }
    
    /**
     * Validates application properties before sending a message.
     *
     * @param properties the application properties to validate
     * @param reservedNames set of reserved property names that users cannot set
     * @param maxAllowedProperties maximum number of properties allowed
     * @param maxPropertiesSize maximum total size in bytes for all properties
     * @throws IllegalArgumentException if validation fails
     */
    public static void validate(
            Map<String, Object> properties,
            Set<String> reservedNames,
            int maxAllowedProperties,
            int maxPropertiesSize) {
        
        if (properties == null || properties.isEmpty()) {
            return;
        }
        
        // Check number of properties
        if (properties.size() > maxAllowedProperties) {
            throw new IllegalArgumentException(
                String.format("Number of application properties (%d) exceeds maximum allowed (%d)",
                    properties.size(), maxAllowedProperties)
            );
        }
        
        // Check for reserved names
        for (String key : properties.keySet()) {
            if (reservedNames.contains(key)) {
                throw new IllegalArgumentException(
                    String.format("Application property name '%s' is reserved and cannot be used", key)
                );
            }
        }
        
        // Check total size of properties
        int totalSize = calculatePropertiesSize(properties);
        if (totalSize > maxPropertiesSize) {
            throw new IllegalArgumentException(
                String.format("Total size of application properties (%d bytes) exceeds maximum allowed (%d bytes)",
                    totalSize, maxPropertiesSize)
            );
        }
    }
    
    /**
     * Calculates the approximate total size of application properties in bytes.
     *
     * @param properties the application properties
     * @return the total size in bytes
     */
    private static int calculatePropertiesSize(Map<String, Object> properties) {
        int totalSize = 0;
        
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            // Key size
            totalSize += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            
            // Value size
            Object value = entry.getValue();
            if (value != null) {
                totalSize += value.toString().getBytes(StandardCharsets.UTF_8).length;
            }
        }
        
        return totalSize;
    }
}
