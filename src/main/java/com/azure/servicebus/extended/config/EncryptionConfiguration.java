package com.azure.servicebus.extended.config;

/**
 * Configuration for blob storage encryption.
 * This is the Azure equivalent of AWS's ServerSideEncryptionStrategy.
 */
public class EncryptionConfiguration {
    
    private String encryptionScope;
    private String customerProvidedKey;
    
    /**
     * Gets the encryption scope to use for blob uploads.
     * Encryption scopes provide a way to manage encryption at the container or blob level.
     *
     * @return the encryption scope name, or null if not configured
     */
    public String getEncryptionScope() {
        return encryptionScope;
    }
    
    public void setEncryptionScope(String encryptionScope) {
        this.encryptionScope = encryptionScope;
    }
    
    /**
     * Gets the customer-provided encryption key (CPK) for blob uploads.
     * When set, blobs will be encrypted with this key.
     *
     * @return the customer-provided key, or null if not configured
     */
    public String getCustomerProvidedKey() {
        return customerProvidedKey;
    }
    
    public void setCustomerProvidedKey(String customerProvidedKey) {
        this.customerProvidedKey = customerProvidedKey;
    }
    
    /**
     * Checks if any encryption is configured.
     *
     * @return true if encryption is configured, false otherwise
     */
    public boolean isEncryptionEnabled() {
        return encryptionScope != null || customerProvidedKey != null;
    }
}
