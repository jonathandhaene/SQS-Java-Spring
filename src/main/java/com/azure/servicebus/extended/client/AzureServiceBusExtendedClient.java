package com.azure.servicebus.extended.client;

import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.SubQueue;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import com.azure.servicebus.extended.util.RetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

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
    private final RetryHandler retryHandler;
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
        this.retryHandler = new RetryHandler(
                config.getRetryMaxAttempts(),
                config.getRetryBackoffMillis(),
                config.getRetryBackoffMultiplier(),
                config.getRetryMaxBackoffMillis()
        );
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
        retryHandler.executeWithRetry(() -> {
            int payloadSize = messageBody.getBytes(StandardCharsets.UTF_8).length;
            boolean shouldOffload = config.isAlwaysThroughBlob() || 
                                   payloadSize > config.getMessageSizeThreshold();

            ServiceBusMessage message;
            Map<String, Object> properties = new HashMap<>(applicationProperties);

            if (shouldOffload) {
                logger.debug("Message size {} exceeds threshold or alwaysThroughBlob=true. Offloading to blob storage.", payloadSize);
                
                // Generate unique blob name
                String blobName = config.getBlobKeyPrefix() + UUID.randomUUID().toString();
                
                // Store payload in blob with retry
                BlobPointer pointer = retryHandler.executeWithRetry(() -> 
                    payloadStore.storePayload(blobName, messageBody)
                );
                
                // Create message with blob pointer as body
                message = new ServiceBusMessage(pointer.toJson());
                
                // Add metadata properties
                properties.put(ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME, payloadSize);
                properties.put(ExtendedClientConfiguration.BLOB_POINTER_MARKER, "true");
                
                logger.debug("Payload offloaded to blob: {}", pointer);
            } else {
                logger.debug("Message size {} is within threshold. Sending directly.", payloadSize);
                message = new ServiceBusMessage(messageBody);
            }

            // Set application properties
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                message.getApplicationProperties().put(entry.getKey(), entry.getValue());
            }

            senderClient.sendMessage(message);
            logger.debug("Message sent successfully");
        });
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
                    try {
                        ExtendedServiceBusMessage extendedMessage = processReceivedMessage(message);
                        messageHandler.accept(extendedMessage);
                        context.complete();
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
     * @return an ExtendedServiceBusMessage with resolved payload
     */
    private ExtendedServiceBusMessage processReceivedMessage(ServiceBusReceivedMessage message) {
        Map<String, Object> appProperties = new HashMap<>(message.getApplicationProperties());
        boolean isFromBlob = "true".equals(String.valueOf(appProperties.get(ExtendedClientConfiguration.BLOB_POINTER_MARKER)));
        
        String body = message.getBody().toString();
        BlobPointer blobPointer = null;

        if (isFromBlob) {
            logger.debug("Message contains blob pointer. Resolving payload...");
            blobPointer = BlobPointer.fromJson(body);
            
            // Use retry for blob retrieval
            String blobPointerJson = body;
            body = retryHandler.executeWithRetry(() -> 
                payloadStore.getPayload(BlobPointer.fromJson(blobPointerJson))
            );
            
            // Remove internal marker properties
            appProperties.remove(ExtendedClientConfiguration.BLOB_POINTER_MARKER);
            appProperties.remove(ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME);
            
            logger.debug("Payload resolved from blob: {}", blobPointer);
        }

        return new ExtendedServiceBusMessage(
                message.getMessageId(),
                body,
                appProperties,
                isFromBlob,
                blobPointer,
                message.getDeadLetterReason(),
                message.getDeadLetterErrorDescription(),
                message.getDeliveryCount()
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
            retryHandler.executeWithRetry(() -> {
                payloadStore.deletePayload(message.getBlobPointer());
            });
            logger.debug("Blob payload deleted successfully");
        } catch (Exception e) {
            logger.error("Failed to delete blob payload after retries", e);
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
    public List<ExtendedServiceBusMessage> receiveDeadLetterMessages(
            String connectionString,
            String queueName,
            int maxMessages) {
        try (ServiceBusReceiverClient dlqReceiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)
                .buildClient()) {

            List<ExtendedServiceBusMessage> extendedMessages = new ArrayList<>();
            
            dlqReceiverClient.receiveMessages(maxMessages, Duration.ofSeconds(10))
                    .forEach(message -> {
                        ExtendedServiceBusMessage extendedMessage = processReceivedMessage(message);
                        extendedMessages.add(extendedMessage);
                    });

            logger.debug("Received {} messages from dead-letter queue", extendedMessages.size());
            return extendedMessages;
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
            Consumer<ExtendedServiceBusMessage> messageHandler,
            Consumer<ServiceBusErrorContext> errorHandler) {
        
        ServiceBusProcessorClient dlqProcessorClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .queueName(queueName)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)
                .processMessage(context -> {
                    ServiceBusReceivedMessage message = context.getMessage();
                    try {
                        ExtendedServiceBusMessage extendedMessage = processReceivedMessage(message);
                        messageHandler.accept(extendedMessage);
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
