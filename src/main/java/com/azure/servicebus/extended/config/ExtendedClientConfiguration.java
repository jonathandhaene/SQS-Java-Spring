package com.azure.servicebus.extended.config;

import com.azure.servicebus.extended.util.BlobKeyPrefixValidator;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for the Azure Service Bus Extended Client.
 */
@Component
@ConfigurationProperties(prefix = "azure.extended-client")
public class ExtendedClientConfiguration {
    
    /**
     * Default message size threshold: 256 KB (262,144 bytes).
     * Messages larger than this will be offloaded to blob storage.
     */
    public static final int DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144;

    /**
     * Reserved application property name for storing the original payload size (modern).
     */
    public static final String RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize";

    /**
     * Legacy reserved application property name for backward compatibility.
     * Azure equivalent of AWS's SQSLargePayloadSize.
     */
    public static final String LEGACY_RESERVED_ATTRIBUTE_NAME = "ServiceBusLargePayloadSize";

    /**
     * Application property marker indicating the message body contains a blob pointer.
     */
    public static final String BLOB_POINTER_MARKER = "com.azure.servicebus.extended.BlobPointer";

    /**
     * User-agent property name for tracking messages sent through extended client.
     */
    public static final String EXTENDED_CLIENT_USER_AGENT = "ExtendedClientUserAgent";

    /**
     * Maximum number of user-defined application properties allowed.
     * Azure Service Bus allows 10 properties total, 1 reserved for extended client.
     */
    public static final int MAX_ALLOWED_PROPERTIES = 9;

    private int messageSizeThreshold = DEFAULT_MESSAGE_SIZE_THRESHOLD;
    private boolean alwaysThroughBlob = false;
    private boolean cleanupBlobOnDelete = true;
    private String blobKeyPrefix = "";
    private boolean ignorePayloadNotFound = false;
    private boolean useLegacyReservedAttributeName = true;
    private boolean payloadSupportEnabled = true;
    private String blobAccessTier;
    private EncryptionConfiguration encryption = new EncryptionConfiguration();
    private int maxAllowedProperties = MAX_ALLOWED_PROPERTIES;

    /**
     * Gets the message size threshold in bytes.
     * Messages exceeding this size will be offloaded to blob storage.
     *
     * @return the message size threshold
     */
    public int getMessageSizeThreshold() {
        return messageSizeThreshold;
    }

    public void setMessageSizeThreshold(int messageSizeThreshold) {
        this.messageSizeThreshold = messageSizeThreshold;
    }

    /**
     * Indicates whether all messages should be stored in blob storage,
     * regardless of size.
     *
     * @return true if all messages should go through blob, false otherwise
     */
    public boolean isAlwaysThroughBlob() {
        return alwaysThroughBlob;
    }

    public void setAlwaysThroughBlob(boolean alwaysThroughBlob) {
        this.alwaysThroughBlob = alwaysThroughBlob;
    }

    /**
     * Indicates whether blob payloads should be automatically deleted
     * when messages are consumed.
     *
     * @return true if blob cleanup is enabled, false otherwise
     */
    public boolean isCleanupBlobOnDelete() {
        return cleanupBlobOnDelete;
    }

    public void setCleanupBlobOnDelete(boolean cleanupBlobOnDelete) {
        this.cleanupBlobOnDelete = cleanupBlobOnDelete;
    }

    /**
     * Gets the prefix to use for blob names when storing payloads.
     *
     * @return the blob key prefix
     */
    public String getBlobKeyPrefix() {
        return blobKeyPrefix;
    }

    public void setBlobKeyPrefix(String blobKeyPrefix) {
        BlobKeyPrefixValidator.validate(blobKeyPrefix);
        this.blobKeyPrefix = blobKeyPrefix;
    }

    /**
     * Indicates whether to ignore blob payload not found errors.
     * When true, if a blob referenced by a message doesn't exist,
     * the message will still be processed with an empty/null body.
     *
     * @return true if payload not found errors should be ignored, false otherwise
     */
    public boolean isIgnorePayloadNotFound() {
        return ignorePayloadNotFound;
    }

    public void setIgnorePayloadNotFound(boolean ignorePayloadNotFound) {
        this.ignorePayloadNotFound = ignorePayloadNotFound;
    }

    /**
     * Indicates whether to use the legacy reserved attribute name for backward compatibility.
     * When true, uses "ServiceBusLargePayloadSize" instead of "ExtendedPayloadSize".
     *
     * @return true if legacy attribute name should be used, false otherwise
     */
    public boolean isUseLegacyReservedAttributeName() {
        return useLegacyReservedAttributeName;
    }

    public void setUseLegacyReservedAttributeName(boolean useLegacyReservedAttributeName) {
        this.useLegacyReservedAttributeName = useLegacyReservedAttributeName;
    }

    /**
     * Indicates whether payload support is enabled.
     * When false, all messages pass through directly without blob interaction.
     *
     * @return true if payload support is enabled, false otherwise
     */
    public boolean isPayloadSupportEnabled() {
        return payloadSupportEnabled;
    }

    public void setPayloadSupportEnabled(boolean payloadSupportEnabled) {
        this.payloadSupportEnabled = payloadSupportEnabled;
    }

    /**
     * Gets the blob access tier to use for uploaded blobs.
     * Can be "Hot", "Cool", or "Archive".
     *
     * @return the blob access tier, or null if not configured
     */
    public String getBlobAccessTier() {
        return blobAccessTier;
    }

    public void setBlobAccessTier(String blobAccessTier) {
        this.blobAccessTier = blobAccessTier;
    }

    /**
     * Gets the encryption configuration for blob storage.
     *
     * @return the encryption configuration
     */
    public EncryptionConfiguration getEncryption() {
        return encryption;
    }

    public void setEncryption(EncryptionConfiguration encryption) {
        this.encryption = encryption;
    }

    /**
     * Gets the maximum number of user-defined application properties allowed.
     *
     * @return the maximum number of properties
     */
    public int getMaxAllowedProperties() {
        return maxAllowedProperties;
    }

    public void setMaxAllowedProperties(int maxAllowedProperties) {
        this.maxAllowedProperties = maxAllowedProperties;
    }

    /**
     * Gets the reserved attribute name to use based on configuration.
     * Returns either the legacy or modern attribute name.
     *
     * @return the reserved attribute name to use
     */
    public String getReservedAttributeName() {
        return useLegacyReservedAttributeName ? LEGACY_RESERVED_ATTRIBUTE_NAME : RESERVED_ATTRIBUTE_NAME;
    }
}
