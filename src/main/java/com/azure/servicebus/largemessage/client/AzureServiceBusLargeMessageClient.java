/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.client;

import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.SubQueue;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import com.azure.servicebus.largemessage.store.ReceiveOnlyBlobResolver;
import com.azure.servicebus.largemessage.util.ApplicationPropertyValidator;
import com.azure.servicebus.largemessage.util.DuplicateDetectionHelper;
import com.azure.servicebus.largemessage.util.RetryHandler;
import com.azure.servicebus.largemessage.util.TracingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Consumer;

/**
 * Core client for handling large messages with Azure Service Bus.
 * 
 * Automatically offloads large message payloads to Azure Blob Storage when they exceed
 * the configured threshold, and transparently resolves blob-stored payloads when receiving messages.
 */
public class AzureServiceBusLargeMessageClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AzureServiceBusLargeMessageClient.class);

    private final ServiceBusSenderClient senderClient;
    private final ServiceBusReceiverClient receiverClient;
    private final BlobPayloadStore payloadStore;
    private final LargeMessageClientConfiguration config;
    private final RetryHandler retryHandler;
    private final ReceiveOnlyBlobResolver receiveOnlyResolver;
    private ServiceBusProcessorClient processorClient;

    /**
     * Creates a new large message client with connection string (production use).
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param payloadStore     the blob payload store
     * @param config           the large message client configuration
     */
    public AzureServiceBusLargeMessageClient(
            String connectionString,
            String queueName,
            BlobPayloadStore payloadStore,
            LargeMessageClientConfiguration config) {
        this.payloadStore = payloadStore;
        this.config = config;
        this.receiveOnlyResolver = new ReceiveOnlyBlobResolver();
        this.retryHandler = new RetryHandler(
                config.getRetryMaxAttempts(),
                config.getRetryBackoffMillis(),
                config.getRetryBackoffMultiplier(),
                config.getRetryMaxBackoffMillis()
        );

        ServiceBusClientBuilder builder = new ServiceBusClientBuilder()
                .connectionString(connectionString);

        this.senderClient = builder.sender()
                .queueName(queueName)
                .buildClient();

        this.receiverClient = builder.receiver()
                .queueName(queueName)
                .buildClient();

        logger.info("AzureServiceBusLargeMessageClient initialized for queue: {}", queueName);
    }

    /**
     * Creates a new large message client with pre-built clients (for testing).
     *
     * @param senderClient   the Service Bus sender client
     * @param receiverClient the Service Bus receiver client
     * @param payloadStore   the blob payload store
     * @param config         the large message client configuration
     */
    public AzureServiceBusLargeMessageClient(
            ServiceBusSenderClient senderClient,
            ServiceBusReceiverClient receiverClient,
            BlobPayloadStore payloadStore,
            LargeMessageClientConfiguration config) {
        this.senderClient = senderClient;
        this.receiverClient = receiverClient;
        this.payloadStore = payloadStore;
        this.config = config;
        this.receiveOnlyResolver = new ReceiveOnlyBlobResolver();
        this.retryHandler = new RetryHandler(
                config.getRetryMaxAttempts(),
                config.getRetryBackoffMillis(),
                config.getRetryBackoffMultiplier(),
                config.getRetryMaxBackoffMillis()
        );
        logger.info("AzureServiceBusLargeMessageClient initialized with provided clients");
    }

    /**
     * Sends a message with the large message client pattern.
     *
     * @param messageBody the message body to send
     */
    public void sendMessage(String messageBody) {
        sendMessage(messageBody, new HashMap<>());
    }

    /**
     * Sends a message with application properties.
     *
     * @param messageBody           the message body to send
     * @param applicationProperties custom application properties
     */
    public void sendMessage(String messageBody, Map<String, Object> applicationProperties) {
        sendMessage(messageBody, null, applicationProperties);
    }

    /**
     * Sends a message with session ID.
     *
     * @param messageBody the message body to send
     * @param sessionId   the session ID
     */
    public void sendMessage(String messageBody, String sessionId) {
        sendMessage(messageBody, sessionId, new HashMap<>());
    }

    /**
     * Sends a message with session ID and application properties.
     *
     * @param messageBody           the message body to send
     * @param sessionId            the session ID (null for non-session messages)
     * @param applicationProperties custom application properties
     */
    public void sendMessage(String messageBody, String sessionId, Map<String, Object> applicationProperties) {
        Object span = null;
        try {
            // Start tracing span if enabled
            if (config.isTracingEnabled() && TracingHelper.isAvailable()) {
                span = TracingHelper.startSendSpan("ServiceBus.send");
                TracingHelper.addAttribute(span, "messaging.system", "servicebus");
            }

            retryHandler.executeWithRetry(() -> {
                // Validate application properties
                ApplicationPropertyValidator.validate(applicationProperties, config.getMaxAllowedProperties());

                // Determine if message should be offloaded using configured criteria
                boolean shouldOffload = config.isPayloadSupportEnabled() && 
                                       config.getMessageSizeCriteria().shouldOffload(messageBody, applicationProperties);

                ServiceBusMessage message;
                Map<String, Object> properties = new HashMap<>(applicationProperties);
                BlobPointer pointer = null;

                if (shouldOffload) {
                    int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
                    logger.debug("Message size {} meets offload criteria. Offloading to blob storage.", payloadSize);
                    
                    // Create message object for blob name resolution
                    ServiceBusMessage tempMessage = new ServiceBusMessage(messageBody);
                    if (sessionId != null && !sessionId.isEmpty()) {
                        tempMessage.setSessionId(sessionId);
                    }
                    
                    // Generate blob name using configured resolver
                    String blobName = config.getBlobNameResolver().resolve(tempMessage);
                    
                    // Store payload in blob with retry
                    pointer = retryHandler.executeWithRetry(() -> 
                        payloadStore.storePayload(blobName, messageBody)
                    );
                    
                    // Generate SAS URI if enabled
                    if (config.isSasEnabled()) {
                        try {
                            String sasUri = payloadStore.generateSasUri(pointer, config.getSasTokenValidationTime());
                            properties.put(config.getMessagePropertyForBlobSasUri(), sasUri);
                            logger.debug("Generated SAS URI and added to message properties");
                        } catch (Exception e) {
                            logger.error("Failed to generate SAS URI, proceeding without it", e);
                            // Continue without SAS URI - receiver can still use storage credentials if available
                        }
                    }
                    
                    // Create message with blob pointer as body (using configured body replacer)
                    String replacementBody = config.getBodyReplacer().replace(messageBody, pointer);
                    message = new ServiceBusMessage(replacementBody);
                    
                    // Add metadata properties using configured attribute name
                    properties.put(config.getReservedAttributeName(), payloadSize);
                    properties.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
                    
                    logger.debug("Payload offloaded to blob: {}", pointer);
                } else {
                    logger.debug("Message does not meet offload criteria. Sending directly.");
                    message = new ServiceBusMessage(messageBody);
                }

                // Set session ID if provided
                if (sessionId != null && !sessionId.isEmpty()) {
                    message.setSessionId(sessionId);
                    logger.debug("Message session ID set to: {}", sessionId);
                }

                // Compute duplicate detection ID if enabled
                if (config.isEnableDuplicateDetectionId()) {
                    String messageId = DuplicateDetectionHelper.computeContentHash(messageBody);
                    message.setMessageId(messageId);
                    logger.debug("Duplicate detection enabled. Message ID set to content hash: {}", messageId);
                }

                // Add user-agent header
                properties.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, 
                              LargeMessageClientConfiguration.USER_AGENT_VALUE);

                // Inject trace context if tracing is enabled
                if (config.isTracingEnabled()) {
                    TracingHelper.injectTraceContext(properties);
                }

                // Set application properties
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    message.getApplicationProperties().put(entry.getKey(), entry.getValue());
                }

                // Transactional atomicity: if sending fails after blob upload, clean up orphaned blob
                try {
                    senderClient.sendMessage(message);
                    logger.debug("Message sent successfully");
                } catch (Exception sendEx) {
                    if (pointer != null) {
                        logger.warn("Service Bus send failed after blob upload. Cleaning up orphaned blob: {}", pointer);
                        try {
                            payloadStore.deletePayload(pointer);
                            logger.debug("Orphaned blob cleaned up successfully: {}", pointer);
                        } catch (Exception cleanupEx) {
                            logger.error("Failed to clean up orphaned blob: {}. Manual cleanup may be required.", pointer, cleanupEx);
                        }
                    }
                    throw sendEx;
                }
            });

            if (span != null) {
                TracingHelper.endSpan(span);
            }
        } catch (Exception e) {
            if (span != null) {
                TracingHelper.recordException(span, e);
                TracingHelper.endSpan(span);
            }
            throw e;
        }
    }

    /**
     * Sends a binary message with the large message client pattern.
     * Supports byte[] payloads (e.g., Avro, Protobuf, or other binary formats).
     *
     * @param payload     the binary payload to send
     * @param contentType the MIME content type (e.g., "application/octet-stream", "application/avro")
     */
    public void sendBinaryMessage(byte[] payload, String contentType) {
        sendBinaryMessage(payload, contentType, new HashMap<>());
    }

    /**
     * Sends a binary message with application properties.
     *
     * @param payload               the binary payload to send
     * @param contentType           the MIME content type
     * @param applicationProperties custom application properties
     */
    public void sendBinaryMessage(byte[] payload, String contentType, Map<String, Object> applicationProperties) {
        Object span = null;
        try {
            if (config.isTracingEnabled() && TracingHelper.isAvailable()) {
                span = TracingHelper.startSendSpan("ServiceBus.sendBinary");
                TracingHelper.addAttribute(span, "messaging.system", "servicebus");
            }

            retryHandler.executeWithRetry(() -> {
                ApplicationPropertyValidator.validate(applicationProperties, config.getMaxAllowedProperties());

                boolean shouldOffload = config.isPayloadSupportEnabled() &&
                        payload.length > config.getMessageSizeThreshold();

                ServiceBusMessage message;
                Map<String, Object> properties = new HashMap<>(applicationProperties);
                BlobPointer pointer = null;

                if (shouldOffload) {
                    logger.debug("Binary message size {} meets offload criteria. Offloading to blob.", payload.length);

                    ServiceBusMessage tempMessage = new ServiceBusMessage(com.azure.core.util.BinaryData.fromBytes(payload));
                    String blobName = config.getBlobNameResolver().resolve(tempMessage);

                    pointer = retryHandler.executeWithRetry(() ->
                        payloadStore.storeBinaryPayload(blobName, payload, contentType)
                    );

                    if (config.isSasEnabled()) {
                        try {
                            String sasUri = payloadStore.generateSasUri(pointer, config.getSasTokenValidationTime());
                            properties.put(config.getMessagePropertyForBlobSasUri(), sasUri);
                        } catch (Exception e) {
                            logger.error("Failed to generate SAS URI for binary message", e);
                        }
                    }

                    String replacementBody = config.getBodyReplacer().replace("", pointer);
                    message = new ServiceBusMessage(replacementBody);

                    properties.put(config.getReservedAttributeName(), payload.length);
                    properties.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
                    properties.put("contentType", contentType);
                } else {
                    message = new ServiceBusMessage(com.azure.core.util.BinaryData.fromBytes(payload));
                    properties.put("contentType", contentType);
                }

                if (config.isEnableDuplicateDetectionId()) {
                    String messageId = DuplicateDetectionHelper.computeContentHash(new String(payload, StandardCharsets.UTF_8));
                    message.setMessageId(messageId);
                }

                properties.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                              LargeMessageClientConfiguration.USER_AGENT_VALUE);

                if (config.isTracingEnabled()) {
                    TracingHelper.injectTraceContext(properties);
                }

                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    message.getApplicationProperties().put(entry.getKey(), entry.getValue());
                }

                // Transactional atomicity: clean up orphaned blob on send failure
                try {
                    senderClient.sendMessage(message);
                    logger.debug("Binary message sent successfully");
                } catch (Exception sendEx) {
                    if (pointer != null) {
                        logger.warn("Service Bus send failed after blob upload. Cleaning up orphaned blob: {}", pointer);
                        try {
                            payloadStore.deletePayload(pointer);
                        } catch (Exception cleanupEx) {
                            logger.error("Failed to clean up orphaned blob: {}", pointer, cleanupEx);
                        }
                    }
                    throw sendEx;
                }
            });

            if (span != null) {
                TracingHelper.endSpan(span);
            }
        } catch (Exception e) {
            if (span != null) {
                TracingHelper.recordException(span, e);
                TracingHelper.endSpan(span);
            }
            throw e;
        }
    }

    /**
     * Sends multiple messages in batch.
     *
     * @param messageBodies list of message bodies to send
     */
    public void sendMessages(List<String> messageBodies) {
        for (String body : messageBodies) {
            sendMessage(body);
        }
    }

    /**
     * Sends multiple messages using true batch send with ServiceBusMessageBatch.
     * Automatically splits into multiple batches when needed and evaluates each message
     * individually for blob offloading.
     *
     * @param messageBodies list of message bodies to send
     */
    public void sendMessageBatch(List<String> messageBodies) {
        sendMessageBatch(messageBodies, new HashMap<>());
    }

    /**
     * Sends multiple messages using true batch send with ServiceBusMessageBatch.
     * Automatically splits into multiple batches when needed and evaluates each message
     * individually for blob offloading.
     *
     * @param messageBodies         list of message bodies to send
     * @param applicationProperties application properties to apply to all messages
     */
    public void sendMessageBatch(List<String> messageBodies, Map<String, Object> applicationProperties) {
        if (messageBodies == null || messageBodies.isEmpty()) {
            logger.debug("No messages to send in batch");
            return;
        }

        Object span = null;
        try {
            // Start tracing span if enabled
            if (config.isTracingEnabled() && TracingHelper.isAvailable()) {
                span = TracingHelper.startSendSpan("ServiceBus.sendBatch");
                TracingHelper.addAttribute(span, "messaging.system", "servicebus");
                TracingHelper.addAttribute(span, "message.count", String.valueOf(messageBodies.size()));
            }

            // Validate application properties
            ApplicationPropertyValidator.validate(applicationProperties, config.getMaxAllowedProperties());

            retryHandler.executeWithRetry(() -> {
                List<ServiceBusMessage> messagesToSend = new ArrayList<>();

                // Process each message individually for blob offloading
                for (String messageBody : messageBodies) {
                    // Determine if message should be offloaded using configured criteria
                    boolean shouldOffload = config.isPayloadSupportEnabled() &&
                                           config.getMessageSizeCriteria().shouldOffload(messageBody, applicationProperties);

                    ServiceBusMessage message;
                    Map<String, Object> properties = new HashMap<>(applicationProperties);

                    if (shouldOffload) {
                        int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
                        logger.debug("Batch message size {} meets offload criteria. Offloading to blob storage.", payloadSize);
                        
                        // Create message object for blob name resolution
                        ServiceBusMessage tempMessage = new ServiceBusMessage(messageBody);
                        
                        // Generate blob name using configured resolver
                        String blobName = config.getBlobNameResolver().resolve(tempMessage);
                        
                        // Store payload in blob with retry
                        BlobPointer pointer = retryHandler.executeWithRetry(() -> 
                            payloadStore.storePayload(blobName, messageBody)
                        );
                        
                        // Generate SAS URI if enabled (now supported in batch send)
                        if (config.isSasEnabled()) {
                            try {
                                String sasUri = payloadStore.generateSasUri(pointer, config.getSasTokenValidationTime());
                                properties.put(config.getMessagePropertyForBlobSasUri(), sasUri);
                                logger.debug("Generated SAS URI for batch message");
                            } catch (Exception e) {
                                logger.error("Failed to generate SAS URI for batch message, proceeding without it", e);
                            }
                        }
                        
                        // Create message with blob pointer as body (using configured body replacer)
                        String replacementBody = config.getBodyReplacer().replace(messageBody, pointer);
                        message = new ServiceBusMessage(replacementBody);
                        
                        // Add metadata properties
                        properties.put(config.getReservedAttributeName(), payloadSize);
                        properties.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
                    } else {
                        message = new ServiceBusMessage(messageBody);
                    }

                    // Compute duplicate detection ID if enabled
                    if (config.isEnableDuplicateDetectionId()) {
                        String messageId = DuplicateDetectionHelper.computeContentHash(messageBody);
                        message.setMessageId(messageId);
                    }

                    // Add user-agent header
                    properties.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, 
                                  LargeMessageClientConfiguration.USER_AGENT_VALUE);

                    // Inject trace context if tracing is enabled
                    if (config.isTracingEnabled()) {
                        TracingHelper.injectTraceContext(properties);
                    }

                    // Set application properties
                    for (Map.Entry<String, Object> entry : properties.entrySet()) {
                        message.getApplicationProperties().put(entry.getKey(), entry.getValue());
                    }

                    messagesToSend.add(message);
                }

                // Send messages using ServiceBusMessageBatch with auto-split
                int messageIndex = 0;
                int totalMessages = messagesToSend.size();
                int batchCount = 0;

                while (messageIndex < totalMessages) {
                    ServiceBusMessageBatch currentBatch = senderClient.createMessageBatch();
                    int messagesInCurrentBatch = 0;

                    while (messageIndex < totalMessages) {
                        ServiceBusMessage message = messagesToSend.get(messageIndex);
                        
                        if (currentBatch.tryAddMessage(message)) {
                            messageIndex++;
                            messagesInCurrentBatch++;
                        } else {
                            // Current batch is full, break to send it
                            if (messagesInCurrentBatch == 0) {
                                // Single message doesn't fit in batch, send it individually
                                logger.warn("Message at index {} too large for batch, sending individually", messageIndex);
                                senderClient.sendMessage(message);
                                messageIndex++;
                            }
                            break;
                        }
                    }

                    if (messagesInCurrentBatch > 0) {
                        senderClient.sendMessages(currentBatch);
                        batchCount++;
                        logger.debug("Sent batch {} with {} messages", batchCount, messagesInCurrentBatch);
                    }
                }

                logger.debug("Successfully sent {} messages in {} batch(es)", totalMessages, batchCount);
            });

            if (span != null) {
                TracingHelper.endSpan(span);
            }
        } catch (Exception e) {
            if (span != null) {
                TracingHelper.recordException(span, e);
                TracingHelper.endSpan(span);
            }
            throw e;
        }
    }

    /**
     * Schedules a message to be enqueued at a specific time.
     *
     * @param messageBody   the message body to send
     * @param scheduledTime the time to enqueue the message
     * @return the sequence number of the scheduled message
     */
    public Long sendScheduledMessage(String messageBody, OffsetDateTime scheduledTime) {
        return sendScheduledMessage(messageBody, scheduledTime, new HashMap<>());
    }

    /**
     * Schedules a message with application properties to be enqueued at a specific time.
     *
     * @param messageBody           the message body to send
     * @param scheduledTime         the time to enqueue the message
     * @param applicationProperties custom application properties
     * @return the sequence number of the scheduled message
     */
    public Long sendScheduledMessage(String messageBody, OffsetDateTime scheduledTime, 
                                     Map<String, Object> applicationProperties) {
        Object span = null;
        try {
            // Start tracing span if enabled
            if (config.isTracingEnabled() && TracingHelper.isAvailable()) {
                span = TracingHelper.startSendSpan("ServiceBus.scheduleMessage");
                TracingHelper.addAttribute(span, "messaging.system", "servicebus");
                TracingHelper.addAttribute(span, "scheduled.time", scheduledTime.toString());
            }

            Long sequenceNumber = retryHandler.executeWithRetry(() -> {
                // Validate application properties
                ApplicationPropertyValidator.validate(applicationProperties, config.getMaxAllowedProperties());

                // Determine if message should be offloaded using configured criteria
                boolean shouldOffload = config.isPayloadSupportEnabled() &&
                                       config.getMessageSizeCriteria().shouldOffload(messageBody, applicationProperties);

                ServiceBusMessage message;
                Map<String, Object> properties = new HashMap<>(applicationProperties);

                if (shouldOffload) {
                    int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
                    logger.debug("Scheduled message size {} meets offload criteria. Offloading to blob storage.", payloadSize);
                    
                    // Create message object for blob name resolution
                    ServiceBusMessage tempMessage = new ServiceBusMessage(messageBody);
                    
                    // Generate blob name using configured resolver
                    String blobName = config.getBlobNameResolver().resolve(tempMessage);
                    BlobPointer pointer = retryHandler.executeWithRetry(() -> 
                        payloadStore.storePayload(blobName, messageBody)
                    );
                    
                    // Create message with blob pointer as body (using configured body replacer)
                    String replacementBody = config.getBodyReplacer().replace(messageBody, pointer);
                    message = new ServiceBusMessage(replacementBody);
                    properties.put(config.getReservedAttributeName(), payloadSize);
                    properties.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
                } else {
                    message = new ServiceBusMessage(messageBody);
                }

                // Compute duplicate detection ID if enabled
                if (config.isEnableDuplicateDetectionId()) {
                    String messageId = DuplicateDetectionHelper.computeContentHash(messageBody);
                    message.setMessageId(messageId);
                }

                // Add user-agent header
                properties.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, 
                              LargeMessageClientConfiguration.USER_AGENT_VALUE);

                // Inject trace context if tracing is enabled
                if (config.isTracingEnabled()) {
                    TracingHelper.injectTraceContext(properties);
                }

                // Set application properties
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    message.getApplicationProperties().put(entry.getKey(), entry.getValue());
                }

                // Schedule the message
                Long seqNum = senderClient.scheduleMessage(message, scheduledTime);
                logger.debug("Scheduled message for {} with sequence number: {}", scheduledTime, seqNum);
                return seqNum;
            });

            if (span != null) {
                TracingHelper.endSpan(span);
            }

            return sequenceNumber;
        } catch (Exception e) {
            if (span != null) {
                TracingHelper.recordException(span, e);
                TracingHelper.endSpan(span);
            }
            throw e;
        }
    }

    /**
     * Receives messages from the queue and resolves blob payloads.
     *
     * @param maxMessages maximum number of messages to receive
     * @return list of large Service Bus messages with resolved payloads
     */
    public List<LargeServiceBusMessage> receiveMessages(int maxMessages) {
        try {
            List<LargeServiceBusMessage> largeMessages = new ArrayList<>();
            
            receiverClient.receiveMessages(maxMessages, Duration.ofSeconds(10))
                    .forEach(message -> {
                        LargeServiceBusMessage largeMessage = processReceivedMessage(message);
                        largeMessages.add(largeMessage);
                    });

            logger.debug("Received {} messages", largeMessages.size());
            return largeMessages;
        } catch (Exception e) {
            logger.error("Failed to receive messages", e);
            throw new RuntimeException("Failed to receive messages", e);
        }
    }

    /**
     * Processes messages using a processor with automatic message handling.
     *
     * @param connectionString the Service Bus connection string
     * @param messageHandler   consumer to handle received messages
     * @param errorHandler     consumer to handle errors
     */
    public void processMessages(
            String connectionString,
            Consumer<LargeServiceBusMessage> messageHandler,
            Consumer<ServiceBusErrorContext> errorHandler) {
        
        // Note: This method creates a processor but doesn't store a queue name
        // In a real scenario, you'd need to pass the queue name as parameter
        throw new UnsupportedOperationException(
            "This method requires queue name. Use processMessages(connectionString, queueName, messageHandler, errorHandler) instead");
    }

    /**
     * Processes messages using a processor with automatic message handling.
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param messageHandler   consumer to handle received messages
     * @param errorHandler     consumer to handle errors
     */
    public void processMessages(
            String connectionString,
            String queueName,
            Consumer<LargeServiceBusMessage> messageHandler,
            Consumer<ServiceBusErrorContext> errorHandler) {
        
        if (processorClient != null) {
            logger.warn("Processor already running. Stopping existing processor.");
            processorClient.close();
        }

        processorClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .queueName(queueName)
                .processMessage(context -> {
                    ServiceBusReceivedMessage message = context.getMessage();
                    try {
                        LargeServiceBusMessage largeMessage = processReceivedMessage(message);
                        messageHandler.accept(largeMessage);
                        context.complete();
                        
                        // Auto-cleanup: delete blob payload after successful completion
                        if (config.isAutoCleanupOnComplete() && largeMessage.isPayloadFromBlob() 
                                && largeMessage.getBlobPointer() != null && payloadStore != null) {
                            try {
                                payloadStore.deletePayload(largeMessage.getBlobPointer());
                                logger.debug("Auto-cleanup: deleted blob payload for message {}", message.getMessageId());
                            } catch (Exception cleanupEx) {
                                logger.warn("Auto-cleanup failed for message {}: {}", message.getMessageId(), cleanupEx.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error processing message: {}", message.getMessageId(), e);
                        if (config.isDeadLetterOnFailure()) {
                            try {
                                context.deadLetter(
                                    new DeadLetterOptions()
                                        .setDeadLetterReason(config.getDeadLetterReason())
                                        .setDeadLetterErrorDescription("Processing failed: " + e.getMessage())
                                );
                                logger.info("Message {} dead-lettered due to processing failure", message.getMessageId());
                            } catch (Exception dlqEx) {
                                logger.error("Failed to dead-letter message: {}", message.getMessageId(), dlqEx);
                                throw dlqEx;
                            }
                        } else {
                            throw e;
                        }
                    }
                })
                .processError(errorHandler)
                .buildProcessorClient();

        processorClient.start();
        logger.info("Message processor started for queue: {}", queueName);
    }

    /**
     * Processes a received Service Bus message and resolves blob payload if needed.
     *
     * @param message the received Service Bus message
     * @return a LargeServiceBusMessage with resolved payload
     */
    private LargeServiceBusMessage processReceivedMessage(ServiceBusReceivedMessage message) {
        Object span = null;
        try {
            // Start tracing span if enabled
            if (config.isTracingEnabled() && TracingHelper.isAvailable()) {
                span = TracingHelper.startSendSpan("ServiceBus.receive");
                TracingHelper.addAttribute(span, "messaging.system", "servicebus");
                TracingHelper.addAttribute(span, "message.id", message.getMessageId());
            }

            Map<String, Object> appProperties = new HashMap<>(message.getApplicationProperties());
            
            // Extract trace context if tracing is enabled
            if (config.isTracingEnabled()) {
                TracingHelper.extractTraceContext(appProperties);
            }
            
            boolean isFromBlob = "true".equals(String.valueOf(appProperties.get(LargeMessageClientConfiguration.BLOB_POINTER_MARKER)));
            
            String body = message.getBody().toString();
            BlobPointer blobPointer = null;

            if (isFromBlob && config.isPayloadSupportEnabled()) {
                logger.debug("Message contains blob pointer. Resolving payload...");
                blobPointer = BlobPointer.fromJson(body);
                
                // Check if message has SAS URI property (get once and store)
                Object sasUriObj = appProperties.get(config.getMessagePropertyForBlobSasUri());
                String sasUri = sasUriObj != null ? String.valueOf(sasUriObj) : null;
                
                // Determine how to download the blob
                if (sasUri != null && config.isReceiveOnlyMode()) {
                    // Use SAS URI for download (no storage credentials needed)
                    logger.debug("Receive-only mode: downloading payload using SAS URI");
                    body = retryHandler.executeWithRetry(() -> receiveOnlyResolver.getPayloadBySasUri(sasUri));
                    
                    // Remove SAS URI property from application properties
                    appProperties.remove(config.getMessagePropertyForBlobSasUri());
                } else if (sasUri != null && payloadStore != null) {
                    // Prefer SAS URI if available, even with storage credentials
                    logger.debug("Downloading payload using SAS URI (SAS available)");
                    body = retryHandler.executeWithRetry(() -> receiveOnlyResolver.getPayloadBySasUri(sasUri));
                    
                    // Remove SAS URI property from application properties
                    appProperties.remove(config.getMessagePropertyForBlobSasUri());
                } else if (payloadStore != null) {
                    // Use storage credentials to download
                    logger.debug("Downloading payload using storage credentials");
                    String blobPointerJson = body;
                    body = retryHandler.executeWithRetry(() -> 
                        payloadStore.getPayload(BlobPointer.fromJson(blobPointerJson))
                    );
                } else {
                    logger.error("Cannot download blob payload: no SAS URI and no storage credentials available");
                    throw new IllegalStateException("Cannot download blob payload: no SAS URI and no storage credentials available");
                }
                
                // Handle null payload (ignorePayloadNotFound enabled)
                if (body == null) {
                    logger.warn("Blob payload not found for message {}. Setting body to empty string.", message.getMessageId());
                    body = "";
                }
                
                // Remove internal marker properties
                appProperties.remove(LargeMessageClientConfiguration.BLOB_POINTER_MARKER);
                
                // Remove both legacy and modern reserved attribute names
                appProperties.remove(LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME);
                appProperties.remove(LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME);
                
                logger.debug("Payload resolved from blob: {}", blobPointer);
            }
            
            // Strip user-agent header
            appProperties.remove(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT);

            LargeServiceBusMessage largeMessage = new LargeServiceBusMessage(
                    message.getMessageId(),
                    body,
                    appProperties,
                    isFromBlob,
                    blobPointer,
                    message.getDeadLetterReason(),
                    message.getDeadLetterErrorDescription(),
                    message.getDeliveryCount()
            );

            if (span != null) {
                TracingHelper.endSpan(span);
            }

            return largeMessage;
        } catch (Exception e) {
            if (span != null) {
                TracingHelper.recordException(span, e);
                TracingHelper.endSpan(span);
            }
            throw e;
        }
    }

    /**
     * Deletes the blob payload associated with a message, if applicable.
     *
     * @param message the large Service Bus message
     */
    public void deletePayload(LargeServiceBusMessage message) {
        if (!config.isCleanupBlobOnDelete()) {
            logger.debug("Blob cleanup is disabled. Skipping deletion.");
            return;
        }

        if (!config.isPayloadSupportEnabled()) {
            logger.debug("Payload support is disabled. Skipping deletion.");
            return;
        }

        if (!message.isPayloadFromBlob()) {
            logger.debug("Message was not from blob. Skipping deletion.");
            return;
        }

        if (message.getBlobPointer() == null) {
            logger.warn("Message is marked as from blob but has no blob pointer. Skipping deletion.");
            return;
        }

        try {
            logger.debug("Deleting blob payload: {}", message.getBlobPointer());
            retryHandler.executeWithRetry(() -> {
                payloadStore.deletePayload(message.getBlobPointer());
            });
            logger.debug("Blob payload deleted successfully");
        } catch (Exception e) {
            logger.error("Failed to delete blob payload after retries", e);
        }
    }

    /**
     * Deletes blob payloads for multiple messages in batch with per-message error handling.
     *
     * @param messages list of large Service Bus messages
     * @return count of successfully deleted blobs
     */
    public int deletePayloadBatch(List<LargeServiceBusMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            logger.debug("No messages provided for batch deletion");
            return 0;
        }

        if (!config.isCleanupBlobOnDelete()) {
            logger.debug("Blob cleanup is disabled. Skipping batch deletion.");
            return 0;
        }

        if (!config.isPayloadSupportEnabled()) {
            logger.debug("Payload support is disabled. Skipping batch deletion.");
            return 0;
        }

        int successCount = 0;
        int failureCount = 0;

        for (LargeServiceBusMessage message : messages) {
            try {
                if (message.isPayloadFromBlob() && message.getBlobPointer() != null) {
                    retryHandler.executeWithRetry(() -> {
                        payloadStore.deletePayload(message.getBlobPointer());
                    });
                    successCount++;
                    logger.debug("Deleted blob payload for message: {}", message.getMessageId());
                }
            } catch (Exception e) {
                failureCount++;
                logger.error("Failed to delete blob payload for message {}: {}", 
                           message.getMessageId(), e.getMessage());
            }
        }

        logger.info("Batch delete completed: {} successful, {} failed", successCount, failureCount);
        return successCount;
    }

    /**
     * Renews the lock on a received message.
     *
     * @param message the received message to renew lock for
     * @return the new lock expiration time
     */
    public OffsetDateTime renewMessageLock(ServiceBusReceivedMessage message) {
        try {
            logger.debug("Renewing message lock for message: {}", message.getMessageId());
            OffsetDateTime newLockExpirationTime = receiverClient.renewMessageLock(message);
            logger.debug("Message lock renewed. New expiration: {}", newLockExpirationTime);
            return newLockExpirationTime;
        } catch (Exception e) {
            logger.error("Failed to renew message lock for message: {}", message.getMessageId(), e);
            throw new RuntimeException("Failed to renew message lock", e);
        }
    }

    /**
     * Renews the lock on multiple received messages in batch.
     *
     * @param messages list of received messages to renew locks for
     * @return map of message ID to new lock expiration time
     */
    public Map<String, OffsetDateTime> renewMessageLockBatch(List<ServiceBusReceivedMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            logger.debug("No messages provided for batch lock renewal");
            return new HashMap<>();
        }

        Map<String, OffsetDateTime> results = new HashMap<>();
        int successCount = 0;
        int failureCount = 0;

        for (ServiceBusReceivedMessage message : messages) {
            try {
                OffsetDateTime newLockExpiration = receiverClient.renewMessageLock(message);
                results.put(message.getMessageId(), newLockExpiration);
                successCount++;
                logger.debug("Lock renewed for message: {} (expires: {})", 
                           message.getMessageId(), newLockExpiration);
            } catch (Exception e) {
                failureCount++;
                logger.error("Failed to renew lock for message {}: {}", 
                           message.getMessageId(), e.getMessage());
            }
        }

        logger.info("Batch lock renewal completed: {} successful, {} failed", successCount, failureCount);
        return results;
    }

    /**
     * Defers a message for later processing.
     *
     * @param message the message to defer
     */
    public void deferMessage(ServiceBusReceivedMessage message) {
        try {
            logger.debug("Deferring message: {}", message.getMessageId());
            receiverClient.defer(message);
            logger.debug("Message deferred successfully. Sequence number: {}", message.getSequenceNumber());
        } catch (Exception e) {
            logger.error("Failed to defer message: {}", message.getMessageId(), e);
            throw new RuntimeException("Failed to defer message", e);
        }
    }

    /**
     * Receives a deferred message by sequence number.
     *
     * @param sequenceNumber the sequence number of the deferred message
     * @return the large Service Bus message with resolved payload
     */
    public LargeServiceBusMessage receiveDeferredMessage(long sequenceNumber) {
        try {
            logger.debug("Receiving deferred message with sequence number: {}", sequenceNumber);
            ServiceBusReceivedMessage message = receiverClient.receiveDeferredMessage(sequenceNumber);
            
            if (message == null) {
                logger.warn("No deferred message found with sequence number: {}", sequenceNumber);
                return null;
            }
            
            LargeServiceBusMessage largeMessage = processReceivedMessage(message);
            logger.debug("Deferred message received and processed");
            return largeMessage;
        } catch (Exception e) {
            logger.error("Failed to receive deferred message with sequence number: {}", sequenceNumber, e);
            throw new RuntimeException("Failed to receive deferred message", e);
        }
    }

    /**
     * Receives multiple deferred messages by sequence numbers.
     *
     * @param sequenceNumbers list of sequence numbers of deferred messages
     * @return list of large Service Bus messages with resolved payloads
     */
    public List<LargeServiceBusMessage> receiveDeferredMessages(List<Long> sequenceNumbers) {
        if (sequenceNumbers == null || sequenceNumbers.isEmpty()) {
            logger.debug("No sequence numbers provided for deferred message retrieval");
            return new ArrayList<>();
        }

        try {
            logger.debug("Receiving {} deferred messages", sequenceNumbers.size());
            
            List<LargeServiceBusMessage> largeMessages = new ArrayList<>();
            
            for (Long sequenceNumber : sequenceNumbers) {
                try {
                    ServiceBusReceivedMessage message = receiverClient.receiveDeferredMessage(sequenceNumber);
                    if (message != null) {
                        LargeServiceBusMessage largeMessage = processReceivedMessage(message);
                        largeMessages.add(largeMessage);
                    } else {
                        logger.warn("No deferred message found with sequence number: {}", sequenceNumber);
                    }
                } catch (Exception e) {
                    logger.error("Failed to receive deferred message with sequence number {}: {}", 
                               sequenceNumber, e.getMessage());
                }
            }
            
            logger.debug("Received and processed {} deferred messages", largeMessages.size());
            return largeMessages;
        } catch (Exception e) {
            logger.error("Failed to receive deferred messages", e);
            throw new RuntimeException("Failed to receive deferred messages", e);
        }
    }

    /**
     * Receives messages from the dead-letter queue.
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param maxMessages      maximum number of messages to receive
     * @return list of dead-lettered messages with resolved payloads
     */
    public List<LargeServiceBusMessage> receiveDeadLetterMessages(
            String connectionString,
            String queueName,
            int maxMessages) {
        try (ServiceBusReceiverClient dlqReceiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)
                .buildClient()) {

            List<LargeServiceBusMessage> largeMessages = new ArrayList<>();
            
            dlqReceiverClient.receiveMessages(maxMessages, Duration.ofSeconds(10))
                    .forEach(message -> {
                        LargeServiceBusMessage largeMessage = processReceivedMessage(message);
                        largeMessages.add(largeMessage);
                    });

            logger.debug("Received {} messages from dead-letter queue", largeMessages.size());
            return largeMessages;
        } catch (Exception e) {
            logger.error("Failed to receive dead-letter messages", e);
            throw new RuntimeException("Failed to receive dead-letter messages", e);
        }
    }

    /**
     * Processes messages from the dead-letter queue using a processor.
     * Note: The returned processor is started but the caller is responsible for
     * stopping it when done by calling close() on the returned client.
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param messageHandler   consumer to handle received dead-letter messages
     * @param errorHandler     consumer to handle errors
     * @return the started processor client that must be closed by the caller
     */
    public ServiceBusProcessorClient processDeadLetterMessages(
            String connectionString,
            String queueName,
            Consumer<LargeServiceBusMessage> messageHandler,
            Consumer<ServiceBusErrorContext> errorHandler) {
        
        ServiceBusProcessorClient dlqProcessorClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .queueName(queueName)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)
                .processMessage(context -> {
                    ServiceBusReceivedMessage message = context.getMessage();
                    try {
                        LargeServiceBusMessage largeMessage = processReceivedMessage(message);
                        messageHandler.accept(largeMessage);
                        context.complete();
                    } catch (Exception e) {
                        logger.error("Error processing dead-letter message: {}", message.getMessageId(), e);
                        throw e;
                    }
                })
                .processError(errorHandler)
                .buildProcessorClient();

        dlqProcessorClient.start();
        logger.info("Dead-letter message processor started for queue: {}", queueName);
        return dlqProcessorClient;
    }

    /**
     * Closes all Service Bus clients.
     */
    @Override
    public void close() {
        try {
            if (processorClient != null) {
                processorClient.close();
                logger.info("Processor client closed");
            }
            if (senderClient != null) {
                senderClient.close();
                logger.info("Sender client closed");
            }
            if (receiverClient != null) {
                receiverClient.close();
                logger.info("Receiver client closed");
            }
        } catch (Exception e) {
            logger.error("Error closing clients", e);
        }
    }
}
