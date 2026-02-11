/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.store.BlobNameResolver;
import com.azure.servicebus.largemessage.store.DefaultBlobNameResolver;
import com.azure.servicebus.largemessage.store.DefaultMessageBodyReplacer;
import com.azure.servicebus.largemessage.store.MessageBodyReplacer;
import com.azure.servicebus.largemessage.util.BlobKeyPrefixValidator;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for the Azure Service Bus Large Message Client.
 */
@Component
@ConfigurationProperties(prefix = "azure.servicebus.large-message-client")
public class LargeMessageClientConfiguration {
    
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
    public static final String BLOB_POINTER_MARKER = "com.azure.servicebus.largemessage.BlobPointer";

    /**
     * Large message client user agent identifier.
     */
    public static final String LARGE_MESSAGE_CLIENT_USER_AGENT = "LargeMessageClientUserAgent";

    /**
     * User agent value.
     */
    public static final String USER_AGENT_VALUE = "AzureServiceBusLargeMessageClient/1.0.0-SNAPSHOT";

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
    private boolean receiveOnlyMode = false;
    
    // Validation
    private int maxAllowedProperties = MAX_ALLOWED_PROPERTIES;
    
    // Blob configuration
    private String blobAccessTier = null; // Hot, Cool, Archive
    private int blobTtlDays = 0; // 0 = disabled
    
    // SAS URI configuration
    private boolean sasEnabled = false;
    private java.time.Duration sasTokenValidationTime = java.time.Duration.ofDays(7);
    private String messagePropertyForBlobSasUri = "$attachment.sas.uri";
    
    // Duplicate detection
    private boolean enableDuplicateDetectionId = false;
    
    // Tracing
    private boolean tracingEnabled = true;
    
    // Encryption (nested configuration)
    private EncryptionConfiguration encryption = new EncryptionConfiguration();

    // Content-type metadata for blob payloads
    private String defaultContentType = "text/plain; charset=utf-8";

    // Auto-cleanup: automatically delete blob payloads after message completion
    private boolean autoCleanupOnComplete = false;

    // Scheduled TTL cleanup interval in minutes (0 = disabled)
    private long ttlCleanupIntervalMinutes = 0;
    
    // Custom resolvers and criteria (transient - not serializable from YAML)
    // These are runtime-injected beans that should not be persisted to configuration files
    private transient BlobNameResolver blobNameResolver;
    private transient MessageBodyReplacer bodyReplacer;
    private transient MessageSizeCriteria messageSizeCriteria;

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
     * Indicates whether receive-only mode is enabled.
     * In receive-only mode, messages can be received using SAS URIs without storage credentials.
     *
     * @return true if receive-only mode is enabled, false otherwise
     */
    public boolean isReceiveOnlyMode() {
        return receiveOnlyMode;
    }

    public void setReceiveOnlyMode(boolean receiveOnlyMode) {
        this.receiveOnlyMode = receiveOnlyMode;
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

    /**
     * Indicates whether SAS URI generation is enabled.
     *
     * @return true if SAS URI generation is enabled, false otherwise
     */
    public boolean isSasEnabled() {
        return sasEnabled;
    }

    public void setSasEnabled(boolean sasEnabled) {
        this.sasEnabled = sasEnabled;
    }

    /**
     * Gets the SAS token validation time duration.
     *
     * @return the SAS token validation time
     */
    public java.time.Duration getSasTokenValidationTime() {
        return sasTokenValidationTime;
    }

    public void setSasTokenValidationTime(java.time.Duration sasTokenValidationTime) {
        this.sasTokenValidationTime = sasTokenValidationTime;
    }

    /**
     * Gets the message property name for storing blob SAS URI.
     *
     * @return the message property name for SAS URI
     */
    public String getMessagePropertyForBlobSasUri() {
        return messagePropertyForBlobSasUri;
    }

    public void setMessagePropertyForBlobSasUri(String messagePropertyForBlobSasUri) {
        this.messagePropertyForBlobSasUri = messagePropertyForBlobSasUri;
    }

    /**
     * Gets the blob name resolver.
     * Returns the default resolver if none is set.
     *
     * @return the blob name resolver
     */
    public BlobNameResolver getBlobNameResolver() {
        if (blobNameResolver == null) {
            blobNameResolver = new DefaultBlobNameResolver(blobKeyPrefix);
        }
        return blobNameResolver;
    }

    public void setBlobNameResolver(BlobNameResolver blobNameResolver) {
        this.blobNameResolver = blobNameResolver;
    }

    /**
     * Gets the message body replacer.
     * Returns the default replacer if none is set.
     *
     * @return the message body replacer
     */
    public MessageBodyReplacer getBodyReplacer() {
        if (bodyReplacer == null) {
            bodyReplacer = new DefaultMessageBodyReplacer();
        }
        return bodyReplacer;
    }

    public void setBodyReplacer(MessageBodyReplacer bodyReplacer) {
        this.bodyReplacer = bodyReplacer;
    }

    /**
     * Gets the message size criteria.
     * Returns the default criteria if none is set.
     *
     * @return the message size criteria
     */
    public MessageSizeCriteria getMessageSizeCriteria() {
        if (messageSizeCriteria == null) {
            messageSizeCriteria = new DefaultMessageSizeCriteria(messageSizeThreshold, alwaysThroughBlob);
        }
        return messageSizeCriteria;
    }

    public void setMessageSizeCriteria(MessageSizeCriteria messageSizeCriteria) {
        this.messageSizeCriteria = messageSizeCriteria;
    }

    /**
     * Gets the default content type to store in blob metadata.
     *
     * @return the default content type
     */
    public String getDefaultContentType() {
        return defaultContentType;
    }

    public void setDefaultContentType(String defaultContentType) {
        this.defaultContentType = defaultContentType;
    }

    /**
     * Indicates whether blob payloads should be automatically deleted
     * when messages are completed in the processor.
     *
     * @return true if auto-cleanup is enabled, false otherwise
     */
    public boolean isAutoCleanupOnComplete() {
        return autoCleanupOnComplete;
    }

    public void setAutoCleanupOnComplete(boolean autoCleanupOnComplete) {
        this.autoCleanupOnComplete = autoCleanupOnComplete;
    }

    /**
     * Gets the interval in minutes for scheduled TTL cleanup of expired blobs.
     * A value of 0 disables scheduled cleanup.
     *
     * @return the cleanup interval in minutes
     */
    public long getTtlCleanupIntervalMinutes() {
        return ttlCleanupIntervalMinutes;
    }

    public void setTtlCleanupIntervalMinutes(long ttlCleanupIntervalMinutes) {
        this.ttlCleanupIntervalMinutes = ttlCleanupIntervalMinutes;
    }
}
