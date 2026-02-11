/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.example;

import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example Spring Boot application demonstrating the Azure Service Bus Large Message Client.
 * 
 * This application shows how to:
 * - Send large messages (automatically offloaded to blob storage)
 * - Send small messages (sent directly)
 * - Receive and process messages
 * - Clean up blob payloads
 * - Retry logic (automatic on send/receive operations)
 * - Dead Letter Queue (DLQ) support
 */
@SpringBootApplication(scanBasePackages = "com.azure.servicebus.largemessage")
public class ExampleApplication {
    private static final Logger logger = LoggerFactory.getLogger(ExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }

    @Bean
    public CommandLineRunner demo(AzureServiceBusLargeMessageClient client) {
        return args -> {
            try {
                logger.info("=== Azure Service Bus Large Message Client Example ===");

                // Example 1: Send a large message (will be automatically offloaded to blob)
                logger.info("\n--- Example 1: Sending Large Message ---");
                String largeMessage = generateLargeMessage(300 * 1024); // 300 KB
                logger.info("Sending large message of size: {} bytes", largeMessage.length());
                
                Map<String, Object> customProperties = new HashMap<>();
                customProperties.put("messageType", "large");
                customProperties.put("timestamp", System.currentTimeMillis());
                
                // Note: Retry logic is automatically applied on send operations
                // If sending fails, it will retry up to retryMaxAttempts times
                // with exponential backoff configured in application.yml
                client.sendMessage(largeMessage, customProperties);
                logger.info("Large message sent successfully (should be in blob storage)");

                // Example 2: Send a small message (will be sent directly)
                logger.info("\n--- Example 2: Sending Small Message ---");
                String smallMessage = "This is a small test message";
                logger.info("Sending small message of size: {} bytes", smallMessage.length());
                client.sendMessage(smallMessage);
                logger.info("Small message sent successfully (should be sent directly)");

                // Give some time for messages to be available
                Thread.sleep(2000);

                // Example 3: Receive and process messages
                logger.info("\n--- Example 3: Receiving Messages ---");
                List<LargeServiceBusMessage> messages = client.receiveMessages(10);
                logger.info("Received {} messages", messages.size());

                for (LargeServiceBusMessage message : messages) {
                    logger.info("Processing message: {}", message.getMessageId());
                    logger.info("  - Body length: {} bytes", message.getBody().length());
                    logger.info("  - From blob: {}", message.isPayloadFromBlob());
                    logger.info("  - Delivery count: {}", message.getDeliveryCount());
                    logger.info("  - Application properties: {}", message.getApplicationProperties());

                    // Example 4: Clean up blob payload (if enabled in config)
                    if (message.isPayloadFromBlob()) {
                        logger.info("  - Cleaning up blob payload: {}", message.getBlobPointer());
                        // Note: Delete operations also use retry logic automatically
                        client.deletePayload(message);
                    }
                }

                logger.info("\n=== Example completed successfully ===");
                
                // Note: The following examples demonstrate DLQ functionality
                // In a real scenario, you would use these methods when you have messages
                // that have been dead-lettered due to processing failures
                
                logger.info("\n--- DLQ Example: Receiving from Dead Letter Queue ---");
                logger.info("Note: This is a demonstration. In production, use this when messages fail processing.");
                logger.info("Dead-lettered messages can be received using:");
                logger.info("  client.receiveDeadLetterMessages(connectionString, queueName, maxMessages)");
                logger.info("Or processed continuously using:");
                logger.info("  client.processDeadLetterMessages(connectionString, queueName, handler, errorHandler)");
                logger.info("When processing messages with processMessages(), failed messages are automatically");
                logger.info("  dead-lettered if deadLetterOnFailure is enabled in configuration.");
                
            } catch (Exception e) {
                logger.error("Error in example application", e);
            } finally {
                // Note: In a real application, you might want to keep the client open
                // For this example, we'll close it after processing
                logger.info("Closing large message client...");
                client.close();
            }
        };
    }

    /**
     * Generates a large message for testing purposes.
     *
     * @param sizeInBytes the desired size in bytes
     * @return a string of the specified size
     */
    private String generateLargeMessage(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        String pattern = "This is a test message payload. ";
        
        while (sb.length() < sizeInBytes) {
            sb.append(pattern);
        }
        
        return sb.substring(0, sizeInBytes);
    }
}
