/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.storage.blob.models.CustomerProvidedKey;

import java.util.Base64;

/**
 * Configuration for server-side encryption of blob payloads.
 *
 * <p>Supports two encryption mechanisms:</p>
 * <ul>
 *   <li><b>Encryption scope</b>: Uses an Azure-managed encryption scope configured on the storage account.</li>
 *   <li><b>Customer-provided key (CPK)</b>: A Base64-encoded 256-bit AES key that Azure Storage uses
 *       for server-side encryption. The key is sent with each request and is never stored by Azure.</li>
 * </ul>
 *
 * <p><b>Disclaimer:</b> This is for illustration purposes only. In production, customer-provided keys
 * should be sourced from a secure key vault, not from configuration files.</p>
 */
public class EncryptionConfiguration {
    
    private String encryptionScope;
    private String customerProvidedKey;
    private String customerProvidedKeySha256;

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
     * Gets the customer-provided encryption key (Base64-encoded 256-bit AES key).
     *
     * @return the customer-provided key
     */
    public String getCustomerProvidedKey() {
        return customerProvidedKey;
    }

    public void setCustomerProvidedKey(String customerProvidedKey) {
        this.customerProvidedKey = customerProvidedKey;
    }

    /**
     * Gets the SHA-256 hash of the customer-provided key (Base64-encoded).
     * If not explicitly set, it is computed from the customer-provided key.
     *
     * @return the SHA-256 hash of the customer-provided key
     */
    public String getCustomerProvidedKeySha256() {
        return customerProvidedKeySha256;
    }

    public void setCustomerProvidedKeySha256(String customerProvidedKeySha256) {
        this.customerProvidedKeySha256 = customerProvidedKeySha256;
    }

    /**
     * Builds an Azure SDK {@link CustomerProvidedKey} from the configured key.
     *
     * @return a {@link CustomerProvidedKey} instance, or {@code null} if no key is configured
     */
    public CustomerProvidedKey toSdkCustomerProvidedKey() {
        if (customerProvidedKey == null || customerProvidedKey.isEmpty()) {
            return null;
        }
        return new CustomerProvidedKey(customerProvidedKey);
    }

    /**
     * Returns whether a customer-provided key is configured and non-empty.
     *
     * @return true if a CPK is configured
     */
    public boolean hasCustomerProvidedKey() {
        return customerProvidedKey != null && !customerProvidedKey.isEmpty();
    }

    /**
     * Returns whether an encryption scope is configured and non-empty.
     *
     * @return true if an encryption scope is configured
     */
    public boolean hasEncryptionScope() {
        return encryptionScope != null && !encryptionScope.isEmpty();
    }
}
