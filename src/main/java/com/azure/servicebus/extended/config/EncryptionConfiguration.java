package com.azure.servicebus.extended.config;

/**
 * Configuration for server-side encryption of blob payloads.
 */
public class EncryptionConfiguration {
    
    private String encryptionScope;
    private String customerProvidedKey;

    /**
     * Gets the encryption scope for blob storage.
     *
     * @return the encryption scope
     */
    public String getEncryptionScope() {
        return encryptionScope;
    }

    public void setEncryptionScope(String encryptionScope) {
        this.encryptionScope = encryptionScope;
    }

    /**
     * Gets the customer-provided encryption key.
     *
     * @return the customer-provided key
     */
    public String getCustomerProvidedKey() {
        return customerProvidedKey;
    }

    public void setCustomerProvidedKey(String customerProvidedKey) {
        this.customerProvidedKey = customerProvidedKey;
    }
}
