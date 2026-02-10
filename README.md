# Azure Service Bus Extended Client Library for Java Spring Boot

> ⚠️ **DISCLAIMER: FOR ILLUSTRATION PURPOSES ONLY**
> 
> This project is a **proof-of-concept / illustrative example** and is **NOT production-ready code**.
> It is provided "as is" without warranty of any kind. The author(s) accept **no responsibility or liability**
> for any issues, damages, or consequences arising from the use of this software.
> **Do not use this code in a production environment** without thorough review, testing, and modification.

An illustrative example library that demonstrates how to transparently offload large message payloads to Azure Blob Storage, similar to the [AWS SQS Extended Client Library](https://github.com/awslabs/amazon-sqs-java-extended-client-lib).

## Overview

Azure Service Bus has a message size limit (256 KB in standard tier). This library automatically handles large messages by:

1. **Sending**: If a message body exceeds the threshold (default 256 KB), it stores the payload in Azure Blob Storage and sends only a pointer via Service Bus
2. **Receiving**: Transparently resolves blob-stored payloads back to the original message body
3. **Cleanup**: Optionally deletes blob payloads when messages are consumed

## Architecture

```
┌─────────────┐                    ┌──────────────────┐
│   Sender    │                    │    Receiver      │
│ Application │                    │   Application    │
└──────┬──────┘                    └────────▲─────────┘
       │                                    │
       │ Large Message                      │ Original Message
       │                                    │
       ▼                                    │
┌─────────────────────────────────────────────────────┐
│   Azure Service Bus Extended Client Library         │
│                                                      │
│  ┌────────────────┐         ┌──────────────────┐   │
│  │  Size Check    │         │ Payload Resolver │   │
│  │  & Offload     │         │   & Cleanup      │   │
│  └────────┬───────┘         └────────▲─────────┘   │
└───────────┼──────────────────────────┼──────────────┘
            │                          │
            │ Pointer                  │ Fetch Payload
            ▼                          │
    ┌────────────────┐         ┌──────┴────────┐
    │ Azure Service  │         │ Azure Blob    │
    │     Bus        │         │   Storage     │
    │   (Message     │         │  (Large       │
    │    Queue)      │         │  Payloads)    │
    └────────────────┘         └───────────────┘
```

## AWS to Azure Mapping

| AWS Component | Azure Component | Library Class |
|--------------|----------------|---------------|
| Amazon SQS | Azure Service Bus | `AzureServiceBusExtendedClient` |
| Amazon S3 | Azure Blob Storage | `BlobPayloadStore` |
| `AmazonSQSExtendedClient` | `AzureServiceBusExtendedClient` | Core client |
| `S3BackedPayloadStore` | `BlobPayloadStore` | Storage layer |
| `PayloadS3Pointer` | `BlobPointer` | Payload reference |

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>com.azure.servicebus.extended</groupId>
    <artifactId>azure-servicebus-extended-client-lib</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure Application

**application.yml:**
```yaml
azure:
  servicebus:
    connection-string: ${AZURE_SERVICEBUS_CONNECTION_STRING}
    queue-name: ${AZURE_SERVICEBUS_QUEUE_NAME:my-queue}
  storage:
    connection-string: ${AZURE_STORAGE_CONNECTION_STRING}
    container-name: ${AZURE_STORAGE_CONTAINER_NAME:large-messages}
  extended-client:
    message-size-threshold: 262144    # 256 KB
    always-through-blob: false
    cleanup-blob-on-delete: true
    blob-key-prefix: ""
```

### 3. Send Messages

```java
@Autowired
private AzureServiceBusExtendedClient client;

// Small message - sent directly
client.sendMessage("Small message");

// Large message - automatically offloaded to blob
String largeMessage = generateLargePayload(); // > 256 KB
client.sendMessage(largeMessage);

// With custom properties
Map<String, Object> properties = new HashMap<>();
properties.put("priority", "high");
client.sendMessage(largeMessage, properties);
```

### 4. Receive Messages

```java
// Receive and process messages
List<ExtendedServiceBusMessage> messages = client.receiveMessages(10);

for (ExtendedServiceBusMessage message : messages) {
    // Body is automatically resolved from blob if needed
    String body = message.getBody();
    
    // Check if payload was from blob
    if (message.isPayloadFromBlob()) {
        System.out.println("Message was stored in blob: " + 
                         message.getBlobPointer());
        
        // Clean up blob payload
        client.deletePayload(message);
    }
}
```

### 5. Process Messages (Event-Driven)

```java
client.processMessages(
    connectionString,
    queueName,
    message -> {
        // Handle message (payload automatically resolved)
        System.out.println("Received: " + message.getBody());
        
        // Clean up if needed
        if (message.isPayloadFromBlob()) {
            client.deletePayload(message);
        }
    },
    errorContext -> {
        // Handle errors
        System.err.println("Error: " + errorContext.getException());
    }
);
```

## Retry Logic

The extended client automatically retries operations that fail due to transient errors. This helps handle temporary network issues, service throttling, and other recoverable failures.

### How It Works

- **Exponential Backoff**: Each retry waits longer than the previous one (e.g., 1s, 2s, 4s)
- **Jitter**: Random variance (±25%) prevents thundering herd problems
- **Configurable**: Control max attempts, initial delay, multiplier, and max delay

### Operations with Retry

All critical operations automatically use retry logic:

1. **Message Sending** (`sendMessage()`)
   - Retries Service Bus send operations
   - Retries blob storage uploads for large messages

2. **Blob Operations**
   - Blob payload retrieval (`getPayload()`)
   - Blob payload deletion (`deletePayload()`)

3. **Configuration**

```yaml
azure:
  extended-client:
    retry-max-attempts: 3              # Total attempts (initial + retries)
    retry-backoff-millis: 1000         # Initial backoff: 1 second
    retry-backoff-multiplier: 2.0      # Each retry doubles the delay
    retry-max-backoff-millis: 30000    # Cap delay at 30 seconds
```

### Example Retry Behavior

With default configuration (3 attempts, 1s initial, 2x multiplier):

```
Attempt 1: Immediate
Attempt 2: Wait ~1s (with jitter: 750ms-1250ms)
Attempt 3: Wait ~2s (with jitter: 1500ms-2500ms)
If all fail: throw exception
```

### Logging

Retry attempts are logged automatically:
```
WARN  Operation failed on attempt 1/3. Retrying after 1000 ms. Error: Connection timeout
WARN  Operation failed on attempt 2/3. Retrying after 2000 ms. Error: Connection timeout
ERROR Operation failed after 3 attempts
```

## Dead Letter Queue (DLQ)

The extended client supports Azure Service Bus Dead Letter Queues for handling messages that fail processing.

### Automatic Dead-Lettering

When `deadLetterOnFailure` is enabled (default), failed messages are automatically moved to the DLQ:

```java
client.processMessages(
    connectionString,
    queueName,
    message -> {
        // If this handler throws an exception, the message is dead-lettered
        processBusinessLogic(message);
    },
    errorContext -> {
        logger.error("Error: {}", errorContext.getException());
    }
);
```

### Configuration

```yaml
azure:
  extended-client:
    dead-letter-on-failure: true               # Enable automatic dead-lettering
    dead-letter-reason: "ProcessingFailure"    # Default reason
    max-delivery-count: 10                      # Informational only
```

> **Note**: The `max-delivery-count` property is informational. Actual enforcement must be configured on the Azure Service Bus queue itself.

### Receiving from DLQ

**Batch Receive:**
```java
List<ExtendedServiceBusMessage> dlqMessages = 
    client.receiveDeadLetterMessages(connectionString, queueName, 10);

for (ExtendedServiceBusMessage msg : dlqMessages) {
    logger.info("Dead-letter reason: {}", msg.getDeadLetterReason());
    logger.info("Dead-letter description: {}", msg.getDeadLetterDescription());
    logger.info("Delivery count: {}", msg.getDeliveryCount());
    
    // Handle or reprocess the message
    reprocessMessage(msg);
}
```

**Continuous Processing:**
```java
ServiceBusProcessorClient dlqProcessor = client.processDeadLetterMessages(
    connectionString,
    queueName,
    message -> {
        // Process dead-lettered messages
        logger.info("Reprocessing DLQ message: {}", message.getMessageId());
        handleFailedMessage(message);
    },
    errorContext -> {
        logger.error("DLQ processing error: {}", errorContext.getException());
    }
);

// Important: Stop the processor when done
// dlqProcessor.close();
```

### Dead-Letter Reasons

Common dead-letter reasons:
- `ProcessingFailure` - Handler threw an exception
- `MaxDeliveryCountExceeded` - Message exceeded retry attempts (set on queue)
- `MessageExpired` - Message TTL expired
- Custom reasons can be set in application code

### Message Metadata

Dead-lettered messages include additional metadata:

```java
ExtendedServiceBusMessage message = ...;

// Standard fields
String body = message.getBody();
String messageId = message.getMessageId();
Map<String, Object> properties = message.getApplicationProperties();

// DLQ-specific fields
String reason = message.getDeadLetterReason();           // Why it was dead-lettered
String description = message.getDeadLetterDescription(); // Additional details
long deliveryCount = message.getDeliveryCount();         // Number of delivery attempts
```

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `azure.servicebus.connection-string` | *required* | Service Bus connection string |
| `azure.servicebus.queue-name` | `my-queue` | Queue name |
| `azure.storage.connection-string` | *required* | Blob Storage connection string |
| `azure.storage.container-name` | `large-messages` | Container for large payloads |
| `azure.extended-client.message-size-threshold` | `262144` (256 KB) | Size threshold for offloading |
| `azure.extended-client.always-through-blob` | `false` | Force all messages through blob |
| `azure.extended-client.cleanup-blob-on-delete` | `true` | Auto-delete blob on message delete |
| `azure.extended-client.blob-key-prefix` | `""` | Prefix for blob names |
| `azure.extended-client.retry-max-attempts` | `3` | Maximum number of retry attempts |
| `azure.extended-client.retry-backoff-millis` | `1000` | Initial backoff delay in milliseconds |
| `azure.extended-client.retry-backoff-multiplier` | `2.0` | Backoff multiplier for exponential backoff |
| `azure.extended-client.retry-max-backoff-millis` | `30000` | Maximum backoff delay cap in milliseconds |
| `azure.extended-client.dead-letter-on-failure` | `true` | Whether to dead-letter messages on processing failure |
| `azure.extended-client.dead-letter-reason` | `"ProcessingFailure"` | Default dead-letter reason |
| `azure.extended-client.max-delivery-count` | `10` | Informational only - actual enforcement is on Service Bus queue |

## Key Features

### ✅ Transparent Payload Offloading
- Automatically stores large payloads in blob storage
- Configurable size threshold (default 256 KB)
- Optional "always through blob" mode

### ✅ Seamless Payload Resolution
- Transparently retrieves blob payloads on receive
- No code changes needed to handle large vs. small messages
- Preserves application properties

### ✅ Automatic Cleanup
- Optional blob deletion when messages are consumed
- Graceful handling of already-deleted blobs
- Configurable cleanup behavior

### ✅ Retry Logic
- Automatic retry with exponential backoff for transient failures
- Configurable retry attempts, backoff delays, and multipliers
- Retries applied to: message sends, blob uploads/downloads, blob deletions
- Jitter added to prevent thundering herd problem
- Detailed logging of retry attempts

### ✅ Dead Letter Queue (DLQ) Support
- Automatic dead-lettering of failed messages when processing
- Configurable dead-letter reason and description
- Methods to receive and process messages from DLQ
- Delivery count tracking on messages
- Graceful failure handling with context preservation

### ✅ Spring Boot Integration
- Auto-configuration with `@EnableAutoConfiguration`
- Configuration properties support
- Conditional bean creation

### ⚠️ Illustrative Code
- This code is for illustration/educational purposes only
- Not intended for production use without significant review and hardening
- Demonstrates patterns and architecture, not production-grade error handling
- Use at your own risk — no warranty or liability accepted

### ✅ Full Test Coverage
- Unit tests with Mockito
- Tests for all core scenarios
- Edge case handling verification

## Building the Project

```bash
# Build
mvn clean install

# Run tests
mvn test

# Run example application
mvn spring-boot:run
```

## Environment Variables

Set these environment variables before running:

```bash
export AZURE_SERVICEBUS_CONNECTION_STRING="Endpoint=sb://..."
export AZURE_SERVICEBUS_QUEUE_NAME="my-queue"
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export AZURE_STORAGE_CONTAINER_NAME="large-messages"
```

## Example Application

See [`ExampleApplication.java`](src/main/java/com/azure/servicebus/extended/example/ExampleApplication.java) for a complete working example demonstrating:
- Sending large and small messages
- Receiving and processing messages
- Cleaning up blob payloads
- Using application properties
- Automatic retry behavior on send/receive operations
- Dead Letter Queue (DLQ) support and demonstrations

## Architecture Components

### Core Classes

- **`AzureServiceBusExtendedClient`**: Main client for sending/receiving messages
- **`BlobPayloadStore`**: Handles blob storage operations
- **`BlobPointer`**: JSON-serializable pointer to blob payloads
- **`ExtendedServiceBusMessage`**: Wrapper for received messages with resolved payloads
- **`ExtendedClientConfiguration`**: Configuration properties
- **`AzureExtendedClientAutoConfiguration`**: Spring Boot auto-configuration
- **`RetryHandler`**: Utility for retry logic with exponential backoff

## Thread Safety

> ⚠️ Note: Thread safety has not been rigorously tested. This code is for illustrative purposes only.

The library is designed to be thread-safe:
- `AzureServiceBusExtendedClient` can be safely used from multiple threads
- Blob storage operations are atomic
- Service Bus clients handle concurrency internally

## Error Handling

> ⚠️ Note: This project now includes retry logic and DLQ support for better error handling, but it's still for demonstration purposes. Production use would require comprehensive monitoring, alerting, and operational procedures.

### Built-in Error Handling

- **Retry Logic**: Transient failures automatically retry with exponential backoff
  - Configurable attempts, delays, and backoff multipliers
  - Applies to message sends, blob operations, and more
  - Detailed logging of retry attempts

- **Dead Letter Queue**: Failed messages moved to DLQ for later analysis
  - Automatic dead-lettering on processing failures
  - Configurable dead-letter reasons
  - Methods to receive and reprocess DLQ messages

- **Blob Cleanup**: Blob deletion failures are logged but don't fail message processing
  - Graceful handling of already-deleted blobs (404 errors)
  - Retry logic applied to deletion attempts

- **Logging**: All operations and errors logged with SLF4J
  - Retry attempts logged at WARN level
  - Final failures logged at ERROR level
  - Success operations logged at DEBUG level

## License & Disclaimer

This project is licensed under the MIT License with an additional disclaimer — see the [LICENSE](LICENSE) file.

**This code is for illustration purposes only and is not production-ready. The author(s) accept no responsibility for its use.**

## Contributing

This is an illustrative project. Contributions are welcome but please note that no guarantees 
are made about maintenance or support. If you wish to use this in production, we recommend 
forking and adapting it to your specific needs with proper testing and security review.

## Related Projects

- [AWS SQS Extended Client Library](https://github.com/awslabs/amazon-sqs-java-extended-client-lib) - Original inspiration
- [Azure Service Bus SDK](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/servicebus)
- [Azure Storage Blob SDK](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/storage/azure-storage-blob)

---

## ⚠️ Disclaimer

This repository and all code within it is provided **for illustrative and educational purposes only**. 
It is **not production-ready** and has **not been audited** for security, performance, or reliability.

The author(s):
- Accept **no responsibility** for any use of this code
- Provide **no warranty** of any kind
- Are **not liable** for any damages or issues arising from its use
- Do **not guarantee** maintenance, updates, or support

If you choose to use any part of this code, you do so **entirely at your own risk**.