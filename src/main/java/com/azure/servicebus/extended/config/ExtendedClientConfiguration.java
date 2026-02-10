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
     * Legacy reserved attribute name for backward compatibility.
     */
    public static final String LEGACY_RESERVED_ATTRIBUTE_NAME = "ServiceBusLargePayloadSize";

    /**
     * Application property marker indicating the message body contains a blob pointer.
     */
    public static final String BLOB_POINTER_MARKER = "com.azure.servicebus.extended.BlobPointer";

    /**
     * Extended client user agent identifier.
     */
    public static final String EXTENDED_CLIENT_USER_AGENT = "ExtendedClientUserAgent";

    /**
     * User agent value.
     */
    public static final String USER_AGENT_VALUE = "AzureServiceBusExtendedClient/1.0.0-SNAPSHOT";

    /**
     * Maximum allowed application properties (excluding reserved ones).
     */
    public static final int MAX_ALLOWED_PROPERTIES = 9;

    private int messageSizeThreshold = DEFAULT_MESSAGE_SIZE_THRESHOLD;
    private boolean alwaysThroughBlob = false;
    private boolean cleanupBlobOnDelete = true;
    private String blobKeyPrefix = "";
    
    // Retry configuration
    private int retryMaxAttempts = 3;
    private long retryBackoffMillis = 1000L;
    private double retryBackoffMultiplier = 2.0;
    private long retryMaxBackoffMillis = 30000L;
    
    // Dead Letter Queue configuration
    private boolean deadLetterOnFailure = true;
    private String deadLetterReason = "ProcessingFailure";
    private int maxDeliveryCount = 10;
    
    // Feature toggles
    private boolean payloadSupportEnabled = true;
    private boolean useLegacyReservedAttributeName = true;
    private boolean ignorePayloadNotFound = false;
    
    // Validation
    private int maxAllowedProperties = MAX_ALLOWED_PROPERTIES;
    
    // Blob configuration
    private String blobAccessTier = null; // Hot, Cool, Archive
    private int blobTtlDays = 0; // 0 = disabled
    
    // Duplicate detection
    private boolean enableDuplicateDetectionId = false;
    
    // Tracing
    private boolean tracingEnabled = true;
    
    // Encryption (nested configuration)
    private EncryptionConfiguration encryption = new EncryptionConfiguration();

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
     * Gets the maximum number of retry attempts for operations.
     *
     * @return the maximum retry attempts
     */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public void setRetryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    /**
     * Gets the initial backoff delay in milliseconds for retries.
     *
     * @return the initial backoff delay
     */
    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public void setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = retryBackoffMillis;
    }

    /**
     * Gets the backoff multiplier for exponential backoff.
     *
     * @return the backoff multiplier
     */
    public double getRetryBackoffMultiplier() {
        return retryBackoffMultiplier;
    }

    public void setRetryBackoffMultiplier(double retryBackoffMultiplier) {
        this.retryBackoffMultiplier = retryBackoffMultiplier;
    }

    /**
     * Gets the maximum backoff delay in milliseconds.
     *
     * @return the maximum backoff delay
     */
    public long getRetryMaxBackoffMillis() {
        return retryMaxBackoffMillis;
    }

    public void setRetryMaxBackoffMillis(long retryMaxBackoffMillis) {
        this.retryMaxBackoffMillis = retryMaxBackoffMillis;
    }

    /**
     * Indicates whether messages should be dead-lettered on processing failure.
     *
     * @return true if dead-lettering is enabled, false otherwise
     */
    public boolean isDeadLetterOnFailure() {
        return deadLetterOnFailure;
    }

    public void setDeadLetterOnFailure(boolean deadLetterOnFailure) {
        this.deadLetterOnFailure = deadLetterOnFailure;
    }

    /**
     * Gets the default dead-letter reason.
     *
     * @return the dead-letter reason
     */
    public String getDeadLetterReason() {
        return deadLetterReason;
    }

    public void setDeadLetterReason(String deadLetterReason) {
        this.deadLetterReason = deadLetterReason;
    }

    /**
     * Gets the maximum delivery count for informational purposes.
     * Actual enforcement is on the Service Bus queue configuration.
     *
     * @return the max delivery count
     */
    public int getMaxDeliveryCount() {
        return maxDeliveryCount;
    }

    public void setMaxDeliveryCount(int maxDeliveryCount) {
        this.maxDeliveryCount = maxDeliveryCount;
    }

    /**
     * Indicates whether payload support is enabled.
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
     * Indicates whether to use legacy reserved attribute name for backward compatibility.
     *
     * @return true to use legacy name, false for modern name
     */
    public boolean isUseLegacyReservedAttributeName() {
        return useLegacyReservedAttributeName;
    }

    public void setUseLegacyReservedAttributeName(boolean useLegacyReservedAttributeName) {
        this.useLegacyReservedAttributeName = useLegacyReservedAttributeName;
    }

    /**
     * Gets the reserved attribute name based on configuration.
     *
     * @return the legacy or modern reserved attribute name
     */
    public String getReservedAttributeName() {
        return useLegacyReservedAttributeName ? LEGACY_RESERVED_ATTRIBUTE_NAME : RESERVED_ATTRIBUTE_NAME;
    }

    /**
     * Indicates whether to ignore missing blob payloads.
     *
     * @return true to ignore missing payloads, false to throw exception
     */
    public boolean isIgnorePayloadNotFound() {
        return ignorePayloadNotFound;
    }

    public void setIgnorePayloadNotFound(boolean ignorePayloadNotFound) {
        this.ignorePayloadNotFound = ignorePayloadNotFound;
    }

    /**
     * Gets the maximum allowed application properties count.
     *
     * @return the maximum allowed properties
     */
    public int getMaxAllowedProperties() {
        return maxAllowedProperties;
    }

    public void setMaxAllowedProperties(int maxAllowedProperties) {
        this.maxAllowedProperties = maxAllowedProperties;
    }

    /**
     * Gets the blob access tier (Hot/Cool/Archive).
     *
     * @return the blob access tier, or null if not set
     */
    public String getBlobAccessTier() {
        return blobAccessTier;
    }

    public void setBlobAccessTier(String blobAccessTier) {
        this.blobAccessTier = blobAccessTier;
    }

    /**
     * Gets the blob TTL in days.
     *
     * @return the blob TTL in days, or 0 if disabled
     */
    public int getBlobTtlDays() {
        return blobTtlDays;
    }

    public void setBlobTtlDays(int blobTtlDays) {
        this.blobTtlDays = blobTtlDays;
    }

    /**
     * Indicates whether duplicate detection ID generation is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public boolean isEnableDuplicateDetectionId() {
        return enableDuplicateDetectionId;
    }

    public void setEnableDuplicateDetectionId(boolean enableDuplicateDetectionId) {
        this.enableDuplicateDetectionId = enableDuplicateDetectionId;
    }

    /**
     * Indicates whether tracing is enabled.
     *
     * @return true if tracing is enabled, false otherwise
     */
    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    /**
     * Gets the encryption configuration.
     *
     * @return the encryption configuration
     */
    public EncryptionConfiguration getEncryption() {
        return encryption;
    }

    public void setEncryption(EncryptionConfiguration encryption) {
        this.encryption = encryption;
    }
}
