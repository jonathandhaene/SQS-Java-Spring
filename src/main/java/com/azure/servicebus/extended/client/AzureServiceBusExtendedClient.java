package com.azure.servicebus.extended.client;

import com.azure.messaging.servicebus.*;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import com.azure.servicebus.extended.util.ApplicationPropertyValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Azure Service Bus Extended Client - the core client that implements the extended client pattern.
 * This is the Azure equivalent of Amazon SQS Extended Client.
 * 
 * Automatically offloads large message payloads to Azure Blob Storage when they exceed
 * the configured threshold, and transparently resolves blob-stored payloads when receiving messages.
 */
public class AzureServiceBusExtendedClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AzureServiceBusExtendedClient.class);

    private final ServiceBusSenderClient senderClient;
    private final ServiceBusReceiverClient receiverClient;
    private final BlobPayloadStore payloadStore;
    private final ExtendedClientConfiguration config;
    private ServiceBusProcessorClient processorClient;

    /**
     * Creates a new extended client with connection string (production use).
     *
     * @param connectionString the Service Bus connection string
     * @param queueName        the queue name
     * @param payloadStore     the blob payload store
     * @param config           the extended client configuration
     */
    public AzureServiceBusExtendedClient(
            String connectionString,
            String queueName,
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        this.payloadStore = payloadStore;
        this.config = config;

        ServiceBusClientBuilder builder = new ServiceBusClientBuilder()
                .connectionString(connectionString);

        this.senderClient = builder.sender()
                .queueName(queueName)
                .buildClient();

        this.receiverClient = builder.receiver()
                .queueName(queueName)
                .buildClient();

        logger.info("AzureServiceBusExtendedClient initialized for queue: {}", queueName);
    }

    /**
     * Creates a new extended client with pre-built clients (for testing).
     *
     * @param senderClient   the Service Bus sender client
     * @param receiverClient the Service Bus receiver client
     * @param payloadStore   the blob payload store
     * @param config         the extended client configuration
     */
    public AzureServiceBusExtendedClient(
            ServiceBusSenderClient senderClient,
            ServiceBusReceiverClient receiverClient,
            BlobPayloadStore payloadStore,
            ExtendedClientConfiguration config) {
        this.senderClient = senderClient;
        this.receiverClient = receiverClient;
        this.payloadStore = payloadStore;
        this.config = config;
        logger.info("AzureServiceBusExtendedClient initialized with provided clients");
    }

    /**
     * Sends a message with the extended client pattern.
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
        try {
            // If payload support is disabled, send directly without blob interaction
            if (!config.isPayloadSupportEnabled()) {
                logger.debug("Payload support disabled. Sending message directly.");
                ServiceBusMessage message = new ServiceBusMessage(messageBody);
                for (Map.Entry<String, Object> entry : applicationProperties.entrySet()) {
                    message.getApplicationProperties().put(entry.getKey(), entry.getValue());
                }
                senderClient.sendMessage(message);
                return;
            }
            
            int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
            boolean shouldOffload = config.isAlwaysThroughBlob() || 
                                   payloadSize > config.getMessageSizeThreshold();

            ServiceBusMessage message;
            Map<String, Object> properties = new HashMap<>(applicationProperties);
            
            // Validate application properties before sending
            Set<String> reservedNames = new HashSet<>(Arrays.asList(
                ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME,
                ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME,
                ExtendedClientConfiguration.BLOB_POINTER_MARKER,
                ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT
            ));
            ApplicationPropertyValidator.validate(properties, reservedNames, config.getMaxAllowedProperties());

            if (shouldOffload) {
                logger.debug("Message size {} exceeds threshold or alwaysThroughBlob=true. Offloading to blob storage.", payloadSize);
                
                // Generate unique blob name
                String blobName = config.getBlobKeyPrefix() + UUID.randomUUID().toString();
                
                // Store payload in blob
                BlobPointer pointer = payloadStore.storePayload(blobName, messageBody);
                
                // Create message with blob pointer as body
                message = new ServiceBusMessage(pointer.toJson());
                
                // Add metadata properties using configured attribute name
                String attributeName = config.getReservedAttributeName();
                properties.put(attributeName, payloadSize);
                properties.put(ExtendedClientConfiguration.BLOB_POINTER_MARKER, "true");
                
                logger.debug("Payload offloaded to blob: {}", pointer);
            } else {
                logger.debug("Message size {} is within threshold. Sending directly.", payloadSize);
                message = new ServiceBusMessage(messageBody);
            }

            // Add user-agent tracking
            properties.put(ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT, 
                          "AzureServiceBusExtendedClient/1.0.0-SNAPSHOT");

            // Set application properties
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                message.getApplicationProperties().put(entry.getKey(), entry.getValue());
            }

            senderClient.sendMessage(message);
            logger.debug("Message sent successfully");
        } catch (Exception e) {
            logger.error("Failed to send message", e);
            throw new RuntimeException("Failed to send message", e);
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
     * Sends multiple messages in a true batch with per-message offloading evaluation.
     * This is the Azure equivalent of AWS's sendMessageBatch with per-message threshold checking.
     *
     * @param messageBodies list of message bodies to send
     */
    public void sendMessageBatch(List<String> messageBodies) {
        sendMessageBatch(messageBodies, new HashMap<>());
    }

    /**
     * Sends multiple messages in a true batch with per-message offloading evaluation.
     * Each message is evaluated individually for blob offloading based on size threshold.
     *
     * @param messageBodies list of message bodies to send
     * @param commonProperties application properties to apply to all messages
     */
    public void sendMessageBatch(List<String> messageBodies, Map<String, Object> commonProperties) {
        try {
            logger.debug("Sending message batch of {} messages", messageBodies.size());
            
            ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();
            
            for (String messageBody : messageBodies) {
                ServiceBusMessage message = prepareMessage(messageBody, commonProperties);
                
                // Try to add message to batch
                if (!messageBatch.tryAddMessage(message)) {
                    // Batch is full, send it and create a new one
                    logger.debug("Batch full, sending {} messages", messageBatch.getCount());
                    senderClient.sendMessages(messageBatch);
                    
                    messageBatch = senderClient.createMessageBatch();
                    if (!messageBatch.tryAddMessage(message)) {
                        // Message too large even for empty batch
                        logger.warn("Message too large for batch, sending individually");
                        senderClient.sendMessage(message);
                    }
                }
            }
            
            // Send remaining messages in batch
            if (messageBatch.getCount() > 0) {
                logger.debug("Sending final batch of {} messages", messageBatch.getCount());
                senderClient.sendMessages(messageBatch);
            }
            
            logger.debug("Message batch sent successfully");
        } catch (Exception e) {
            logger.error("Failed to send message batch", e);
            throw new RuntimeException("Failed to send message batch", e);
        }
    }

    /**
     * Prepares a message for sending, applying blob offloading logic if needed.
     *
     * @param messageBody the message body
     * @param applicationProperties application properties to add
     * @return the prepared ServiceBusMessage
     */
    private ServiceBusMessage prepareMessage(String messageBody, Map<String, Object> applicationProperties) {
        // If payload support is disabled, send directly
        if (!config.isPayloadSupportEnabled()) {
            ServiceBusMessage message = new ServiceBusMessage(messageBody);
            for (Map.Entry<String, Object> entry : applicationProperties.entrySet()) {
                message.getApplicationProperties().put(entry.getKey(), entry.getValue());
            }
            return message;
        }
        
        int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
        boolean shouldOffload = config.isAlwaysThroughBlob() || 
                               payloadSize > config.getMessageSizeThreshold();

        ServiceBusMessage message;
        Map<String, Object> properties = new HashMap<>(applicationProperties);
        
        // Validate application properties
        Set<String> reservedNames = new HashSet<>(Arrays.asList(
            ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME,
            ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME,
            ExtendedClientConfiguration.BLOB_POINTER_MARKER,
            ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT
        ));
        ApplicationPropertyValidator.validate(properties, reservedNames, config.getMaxAllowedProperties());

        if (shouldOffload) {
            // Generate unique blob name
            String blobName = config.getBlobKeyPrefix() + UUID.randomUUID().toString();
            
            // Store payload in blob
            BlobPointer pointer = payloadStore.storePayload(blobName, messageBody);
            
            // Create message with blob pointer as body
            message = new ServiceBusMessage(pointer.toJson());
            
            // Add metadata properties
            String attributeName = config.getReservedAttributeName();
            properties.put(attributeName, payloadSize);
            properties.put(ExtendedClientConfiguration.BLOB_POINTER_MARKER, "true");
        } else {
            message = new ServiceBusMessage(messageBody);
        }

        // Add user-agent tracking
        properties.put(ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT, 
                      "AzureServiceBusExtendedClient/1.0.0-SNAPSHOT");

        // Set application properties
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            message.getApplicationProperties().put(entry.getKey(), entry.getValue());
        }

        return message;
    }

    /**
     * Receives messages from the queue and resolves blob payloads.
     *
     * @param maxMessages maximum number of messages to receive
     * @return list of extended Service Bus messages with resolved payloads
     */
    public List<ExtendedServiceBusMessage> receiveMessages(int maxMessages) {
        try {
            List<ExtendedServiceBusMessage> extendedMessages = new ArrayList<>();
            
            receiverClient.receiveMessages(maxMessages, Duration.ofSeconds(10))
                    .forEach(message -> {
                        ExtendedServiceBusMessage extendedMessage = processReceivedMessage(message);
                        extendedMessages.add(extendedMessage);
                    });

            logger.debug("Received {} messages", extendedMessages.size());
            return extendedMessages;
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
            Consumer<ExtendedServiceBusMessage> messageHandler,
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
            Consumer<ExtendedServiceBusMessage> messageHandler,
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
                    ExtendedServiceBusMessage extendedMessage = processReceivedMessage(message);
                    messageHandler.accept(extendedMessage);
                    context.complete();
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
     * @return an ExtendedServiceBusMessage with resolved payload
     */
    private ExtendedServiceBusMessage processReceivedMessage(ServiceBusReceivedMessage message) {
        Map<String, Object> appProperties = new HashMap<>(message.getApplicationProperties());
        
        // Check for blob pointer marker
        boolean isFromBlob = "true".equals(String.valueOf(appProperties.get(ExtendedClientConfiguration.BLOB_POINTER_MARKER)));
        
        String body = message.getBody().toString();
        BlobPointer blobPointer = null;

        if (isFromBlob) {
            logger.debug("Message contains blob pointer. Resolving payload...");
            try {
                blobPointer = BlobPointer.fromJson(body);
                body = payloadStore.getPayload(blobPointer);
                
                // Handle case where blob is not found and ignorePayloadNotFound is true
                if (body == null && config.isIgnorePayloadNotFound()) {
                    logger.warn("Blob payload not found for message {}, returning empty body", message.getMessageId());
                    body = "";
                }
                
                // Remove internal marker properties
                appProperties.remove(ExtendedClientConfiguration.BLOB_POINTER_MARKER);
                
                // Remove both modern and legacy reserved attribute names
                appProperties.remove(ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME);
                appProperties.remove(ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME);
                
                logger.debug("Payload resolved from blob: {}", blobPointer);
            } catch (Exception e) {
                logger.error("Failed to resolve blob payload", e);
                throw new RuntimeException("Failed to resolve blob payload", e);
            }
        }
        
        // Strip user-agent property
        appProperties.remove(ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT);

        return new ExtendedServiceBusMessage(
                message.getMessageId(),
                body,
                appProperties,
                isFromBlob,
                blobPointer
        );
    }

    /**
     * Deletes the blob payload associated with a message, if applicable.
     *
     * @param message the extended Service Bus message
     */
    public void deletePayload(ExtendedServiceBusMessage message) {
        if (!config.isCleanupBlobOnDelete()) {
            logger.debug("Blob cleanup is disabled. Skipping deletion.");
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
            payloadStore.deletePayload(message.getBlobPointer());
            logger.debug("Blob payload deleted successfully");
        } catch (Exception e) {
            logger.error("Failed to delete blob payload", e);
            // Don't throw exception - cleanup failure shouldn't break message processing
        }
    }

    /**
     * Deletes blob payloads for a batch of messages.
     * This is the Azure equivalent of AWS's deleteMessageBatch.
     * Handles per-message errors gracefully - one failure won't stop others.
     *
     * @param messages list of extended Service Bus messages
     */
    public void deletePayloadBatch(List<ExtendedServiceBusMessage> messages) {
        if (!config.isCleanupBlobOnDelete()) {
            logger.debug("Blob cleanup is disabled. Skipping deletion.");
            return;
        }

        logger.debug("Deleting blob payloads for batch of {} messages", messages.size());
        int successCount = 0;
        int skipCount = 0;
        int failCount = 0;

        for (ExtendedServiceBusMessage message : messages) {
            try {
                if (!message.isPayloadFromBlob() || message.getBlobPointer() == null) {
                    skipCount++;
                    continue;
                }

                payloadStore.deletePayload(message.getBlobPointer());
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to delete blob payload for message {}: {}", 
                           message.getMessageId(), e.getMessage());
                failCount++;
                // Continue with other messages
            }
        }

        logger.info("Batch delete completed: {} succeeded, {} skipped, {} failed", 
                   successCount, skipCount, failCount);
    }

    /**
     * Renews the lock on a message.
     * This is the Azure equivalent of AWS's changeMessageVisibility.
     *
     * @param message the received message to renew lock for
     */
    public void renewMessageLock(ServiceBusReceivedMessage message) {
        try {
            logger.debug("Renewing message lock for message: {}", message.getMessageId());
            receiverClient.renewMessageLock(message);
            logger.debug("Message lock renewed successfully");
        } catch (Exception e) {
            logger.error("Failed to renew message lock for message {}", message.getMessageId(), e);
            throw new RuntimeException("Failed to renew message lock", e);
        }
    }

    /**
     * Renews locks for a batch of messages.
     * This is the Azure equivalent of AWS's changeMessageVisibilityBatch.
     *
     * @param messages list of received messages to renew locks for
     */
    public void renewMessageLockBatch(List<ServiceBusReceivedMessage> messages) {
        logger.debug("Renewing message locks for batch of {} messages", messages.size());
        int successCount = 0;
        int failCount = 0;

        for (ServiceBusReceivedMessage message : messages) {
            try {
                receiverClient.renewMessageLock(message);
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to renew message lock for message {}: {}", 
                           message.getMessageId(), e.getMessage());
                failCount++;
                // Continue with other messages
            }
        }

        logger.info("Batch lock renewal completed: {} succeeded, {} failed", 
                   successCount, failCount);
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
