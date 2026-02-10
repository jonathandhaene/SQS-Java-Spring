# Azure Service Bus Large Message Client Guide

## Introduction
This guide provides instructions and examples for using the Azure Service Bus Large Message Client. It covers configuration, payload offloading to Azure Blob Storage, retries, error handling, and common usage patterns.

## Configuration

### Connection Settings

**application.yml:**
```yaml
azure:
  servicebus:
    connection-string: ${AZURE_SERVICEBUS_CONNECTION_STRING}
    queue-name: ${AZURE_SERVICEBUS_QUEUE_NAME:my-queue}
  storage:
    connection-string: ${AZURE_STORAGE_CONNECTION_STRING}
    container-name: ${AZURE_STORAGE_CONTAINER_NAME:large-messages}
  large-message-client:
    message-size-threshold: 262144    # 256 KB
    always-through-blob: false
    cleanup-blob-on-delete: true
    blob-key-prefix: ""
```

### Environment Variables

```bash
e...","AZURE_STORAGE_QUEUE_NAME":"my-queue"}

## Key Features

### 1. Payload Offloading to Azure Blob Storage

Messages exceeding the configured size threshold (default 256 KB) are automatically offloaded to Azure Blob Storage. A pointer is sent via Service Bus instead of the full payload.

```java
@Autowired
private AzureServiceBusLargeMessageClient client;

// Small message — sent directly via Service Bus
client.sendMessage("Small message");

// Large message — automatically offloaded to Blob Storage
String largeMessage = generateLargePayload(); // > 256 KB
client.sendMessage(largeMessage);

// With custom application properties
Map<String, Object> properties = new HashMap<>();
properties.put("priority", "high");
client.sendMessage(largeMessage, properties);
```

### 2. Receiving Messages

Payloads stored in Blob Storage are transparently resolved back to the original message body on receive.

```java
List<LargeServiceBusMessage> messages = client.receiveMessages(10);

for (LargeServiceBusMessage message : messages) {
    // Body is automatically resolved from blob if needed
    String body = message.getBody();

    if (message.isPayloadFromBlob()) {
        System.out.println("Payload was stored in blob: " + message.getBlobPointer());

        // Clean up blob payload after processing
        client.deletePayload(message);
    }
}
```

### 3. Batch Operations

Send multiple messages efficiently using `ServiceBusMessageBatch` with automatic splitting when the batch is full.

```java
// Batch send
List<String> messageBodies = List.of("message1", "message2", "message3");
client.sendMessageBatch(messageBodies);

// Batch send with custom properties
Map<String, Object> properties = Map.of("source", "batch-job");
client.sendMessageBatch(messageBodies, properties);

// Batch delete blob payloads
List<LargeServiceBusMessage> processedMessages = ...;
client.deletePayloadBatch(processedMessages);
```

### 4. Retry Logic

Transient failures are automatically retried with exponential backoff and jitter.

**Configuration:**
```yaml
azure:
  large-message-client:
    retry-max-attempts: 3
    retry-backoff-millis: 1000
    retry-backoff-multiplier: 2.0
    retry-max-backoff-millis: 30000
```

**Programmatic retry options:**
```java
ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
    .connectionString(connectionString)
    .retryOptions(new AmqpRetryOptions()
        .setMaxRetries(3)
        .setMode(AmqpRetryMode.EXPONENTIAL)
        .setMaxDelay(Duration.ofSeconds(30))
    );
```

### 5. Dead Letter Queue (DLQ) Support

Failed messages are automatically moved to the Dead Letter Queue when `deadLetterOnFailure` is enabled.

**Configuration:**
```yaml
azure:
  large-message-client:
    dead-letter-on-failure: true
    dead-letter-reason: "ProcessingFailure"
    max-delivery-count: 10
```

**Processing with automatic dead-lettering:**
```java
client.processMessages(
    connectionString,
    queueName,
    message -> {
        // If this handler throws an exception, the message is dead-lettered
        processBusinessLogic(message);
    },
    errorContext -> {
        System.err.println("Error: " + errorContext.getException());
    }
);
```

**Receiving from the DLQ:**
```java
List<LargeServiceBusMessage> dlqMessages =
    client.receiveDeadLetterMessages(connectionString, queueName, 10);

for (LargeServiceBusMessage msg : dlqMessages) {
    System.out.println("Dead-letter reason: " + msg.getDeadLetterReason());
    System.out.println("Description: " + msg.getDeadLetterDescription());
    System.out.println("Delivery count: " + msg.getDeliveryCount());
}
```

### 6. Scheduled Messages

Schedule messages for future delivery using Azure Service Bus's scheduled enqueue feature.

```java
OffsetDateTime scheduledTime = OffsetDateTime.now().plusMinutes(30);

// Schedule a message
client.sendScheduledMessage("Delayed message body", scheduledTime);

// Schedule with custom properties
Map<String, Object> properties = Map.of("type", "reminder");
client.sendScheduledMessage("Delayed message body", scheduledTime, properties);
```

### 7. Message Deferral

Defer a message for later retrieval by sequence number.

```java
// Defer a message
client.deferMessage(receivedMessage);

// Retrieve a deferred message by sequence number
LargeServiceBusMessage deferred = client.receiveDeferredMessage(sequenceNumber);

// Retrieve multiple deferred messages
List<LargeServiceBusMessage> deferredBatch =
    client.receiveDeferredMessages(List.of(seq1, seq2, seq3));
```

### 8. Session Support

Send messages with session IDs for ordered, grouped processing.

```java
// Send a message with a session ID
client.sendMessage("Order update", "session-order-123");

// Send with session ID and custom properties
Map<String, Object> properties = Map.of("orderType", "express");
client.sendMessage("Order update", "session-order-123", properties);
```

### 9. Message Lock Renewal

Renew message locks to extend processing time for long-running operations.

```java
// Renew lock on a single message
client.renewMessageLock(receivedMessage);

// Renew locks on a batch of messages
client.renewMessageLockBatch(List.of(msg1, msg2, msg3));
```

### 10. Event-Driven Message Processing

Use the built-in processor for continuous, event-driven message consumption.

```java
client.processMessages(
    connectionString,
    queueName,
    message -> {
        System.out.println("Received: " + message.getBody());

        if (message.isPayloadFromBlob()) {
            client.deletePayload(message);
        }
    },
    errorContext -> {
        System.err.println("Error: " + errorContext.getException());
    }
);
```

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `azure.servicebus.connection-string` | *required* | Service Bus connection string |
| `azure.servicebus.queue-name` | `my-queue` | Queue name |
| `azure.storage.connection-string` | *required* | Blob Storage connection string |
| `azure.storage.container-name` | `large-messages` | Container for large payloads |
| `azure.large-message-client.message-size-threshold` | `262144` (256 KB) | Size threshold for offloading |
| `azure.large-message-client.always-through-blob` | `false` | Force all messages through blob |
| `azure.large-message-client.cleanup-blob-on-delete` | `true` | Auto-delete blob on message delete |
| `azure.large-message-client.blob-key-prefix` | `""` | Prefix for blob names |
| `azure.large-message-client.retry-max-attempts` | `3` | Maximum retry attempts |
| `azure.large-message-client.retry-backoff-millis` | `1000` | Initial backoff delay (ms) |
| `azure.large-message-client.retry-backoff-multiplier` | `2.0` | Backoff multiplier |
| `azure.large-message-client.retry-max-backoff-millis` | `30000` | Maximum backoff delay cap (ms) |
| `azure.large-message-client.dead-letter-on-failure` | `true` | Dead-letter messages on failure |
| `azure.large-message-client.dead-letter-reason` | `"ProcessingFailure"` | Default dead-letter reason |
| `azure.large-message-client.max-delivery-count` | `10` | Informational only |

## Conclusion
This guide covers the key features and usage patterns of the Azure Service Bus Large Message Client. For more details, refer to the [README](README.md) and the example application in the repository.
