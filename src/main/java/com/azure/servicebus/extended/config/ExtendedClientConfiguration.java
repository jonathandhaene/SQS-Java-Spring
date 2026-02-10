package com.azure.servicebus.extended.config;

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
     * Reserved application property name for storing the original payload size.
     */
    public static final String RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize";

    /**
     * Application property marker indicating the message body contains a blob pointer.
     */
    public static final String BLOB_POINTER_MARKER = "com.azure.servicebus.extended.BlobPointer";

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
}
