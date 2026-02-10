package com.azure.servicebus.extended.model;

import java.util.Map;

/**
 * Wrapper for received Service Bus messages with extended client support.
 * Contains the message body (resolved from blob if needed) and metadata.
 */
public class ExtendedServiceBusMessage {
    private final String messageId;
    private final String body;
    private final Map<String, Object> applicationProperties;
    private final boolean payloadFromBlob;
    private final BlobPointer blobPointer;
    private final String deadLetterReason;
    private final String deadLetterDescription;
    private final long deliveryCount;

    public ExtendedServiceBusMessage(
            String messageId,
            String body,
            Map<String, Object> applicationProperties,
            boolean payloadFromBlob,
            BlobPointer blobPointer) {
        this(messageId, body, applicationProperties, payloadFromBlob, blobPointer, null, null, 0);
    }

    public ExtendedServiceBusMessage(
            String messageId,
            String body,
            Map<String, Object> applicationProperties,
            boolean payloadFromBlob,
            BlobPointer blobPointer,
            String deadLetterReason,
            String deadLetterDescription,
            long deliveryCount) {
        this.messageId = messageId;
        this.body = body;
        this.applicationProperties = applicationProperties;
        this.payloadFromBlob = payloadFromBlob;
        this.blobPointer = blobPointer;
        this.deadLetterReason = deadLetterReason;
        this.deadLetterDescription = deadLetterDescription;
        this.deliveryCount = deliveryCount;
    }

    /**
     * Gets the unique message identifier.
     *
     * @return the message ID
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Gets the message body (resolved from blob storage if needed).
     *
     * @return the message body
     */
    public String getBody() {
        return body;
    }

    /**
     * Gets the application properties associated with the message.
     *
     * @return the application properties
     */
    public Map<String, Object> getApplicationProperties() {
        return applicationProperties;
    }

    /**
     * Indicates whether the payload was retrieved from blob storage.
     *
     * @return true if payload was from blob, false otherwise
     */
    public boolean isPayloadFromBlob() {
        return payloadFromBlob;
    }

    /**
     * Gets the blob pointer if the payload was stored in blob storage.
     *
     * @return the blob pointer, or null if payload was not from blob
     */
    public BlobPointer getBlobPointer() {
        return blobPointer;
    }

    /**
     * Gets the dead-letter reason if the message was dead-lettered.
     *
     * @return the dead-letter reason, or null if not dead-lettered
     */
    public String getDeadLetterReason() {
        return deadLetterReason;
    }

    /**
     * Gets the dead-letter description if the message was dead-lettered.
     *
     * @return the dead-letter description, or null if not dead-lettered
     */
    public String getDeadLetterDescription() {
        return deadLetterDescription;
    }

    /**
     * Gets the number of times this message has been delivered.
     *
     * @return the delivery count
     */
    public long getDeliveryCount() {
        return deliveryCount;
    }

    @Override
    public String toString() {
        return "ExtendedServiceBusMessage{" +
                "messageId='" + messageId + '\'' +
                ", bodyLength=" + (body != null ? body.length() : 0) +
                ", payloadFromBlob=" + payloadFromBlob +
                ", blobPointer=" + blobPointer +
                ", deliveryCount=" + deliveryCount +
                ", deadLetterReason='" + deadLetterReason + '\'' +
                '}';
    }
}
