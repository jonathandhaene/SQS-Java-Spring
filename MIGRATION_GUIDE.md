# Azure Service Bus Large Message Client Guide

> ⚠️ **DISCLAIMER: FOR ILLUSTRATION PURPOSES ONLY**
> 
> This project is a **proof-of-concept / illustrative example** and is **NOT production-ready code**.
> It is provided "as is" without warranty of any kind. The author(s) accept **no responsibility or liability**
> for any issues, damages, or consequences arising from the use of this software.
> **Do not use this code in a production environment** without thorough review, testing, and modification.

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
    large-message-client:
      message-size-threshold: 262144    # 256 KB
      always-through-blob: false
      cleanup-blob-on-delete: true
      blob-key-prefix: ""
  storage:
    connection-string: ${AZURE_STORAGE_CONNECTION_STRING}
    container-name: ${AZURE_STORAGE_CONTAINER_NAME:large-messages}
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
  servicebus:
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
  servicebus:
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

## Advanced Features

### 11. SAS URI Support

Generate Shared Access Signature (SAS) URIs for secure blob access without requiring storage account credentials on the receiver side.

**Configuration:**
```yaml
azure:
  servicebus:
    large-message-client:
      sas-enabled: true  # Enable SAS URI generation
      sas-token-validation-time: P7D  # ISO-8601 duration (7 days)
      message-property-for-blob-sas-uri: "$attachment.sas.uri"
```

**Usage:**
When SAS is enabled, each uploaded blob automatically gets a SAS URI added as a message property. Receivers can download the blob using only this URI.

### 12. Receive-Only Mode

Configure a client to receive messages using only SAS URIs, without needing storage account credentials.

**Configuration:**
```yaml
azure:
  servicebus:
    large-message-client:
      receive-only-mode: true  # No storage credentials needed
```

**Usage:**
```java
// This client can receive and download blob payloads using SAS URIs only
// No azure.storage.connection-string required
@Autowired
private AzureServiceBusLargeMessageClient client;

List<LargeServiceBusMessage> messages = client.receiveMessages(10);
// Blobs are downloaded using SAS URIs from message properties
```

### 13. Custom Blob Naming Strategy

Implement custom blob naming logic (e.g., `{tenantId}/{messageId}` or date-based naming).

**Create a custom resolver:**
```java
@Component
public class CustomBlobNameResolver implements BlobNameResolver {
    @Override
    public String resolve(ServiceBusMessage message) {
        String tenantId = (String) message.getApplicationProperties().get("tenantId");
        String messageId = message.getMessageId();
        return String.format("%s/%s", tenantId, messageId);
    }
}
```

**Spring Boot auto-configuration will automatically use your custom bean.**

### 14. Custom Body Replacement

Control what the message body becomes after blob offloading (default is BlobPointer JSON).

**Example - Set body to null (like .NET):**
```java
@Component
public class NullBodyReplacer implements MessageBodyReplacer {
    @Override
    public String replace(String originalBody, BlobPointer pointer) {
        return "";  // or null, or custom metadata
    }
}
```

### 15. Dynamic Connection Strings (Key Vault Integration)

Integrate with Azure Key Vault or other dynamic credential sources.

**Example - Key Vault provider:**
```java
@Component
public class KeyVaultConnectionStringProvider implements StorageConnectionStringProvider {
    @Autowired
    private SecretClient keyVaultClient;
    
    @Override
    public String getConnectionString() {
        return keyVaultClient.getSecret("storage-connection-string").getValue();
    }
}
```

**Spring Boot auto-configuration will use your provider instead of static properties.**

### 16. Custom Message Size Criteria

Implement custom logic to determine when messages should be offloaded (beyond simple size thresholds).

**Example - Offload based on message type:**
```java
@Component
public class CustomMessageSizeCriteria implements MessageSizeCriteria {
    @Override
    public boolean shouldOffload(String messageBody, Map<String, Object> properties) {
        String messageType = (String) properties.get("messageType");
        
        // Always offload "reports" type
        if ("report".equals(messageType)) {
            return true;
        }
        
        // Use size threshold for other types
        return messageBody.getBytes(StandardCharsets.UTF_8).length > 100_000;
    }
}
```

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `azure.servicebus.connection-string` | *required* | Service Bus connection string |
| `azure.servicebus.queue-name` | `my-queue` | Queue name |
| `azure.storage.connection-string` | *required* | Blob Storage connection string (not needed in receive-only mode) |
| `azure.storage.container-name` | `large-messages` | Container for large payloads |
| `azure.servicebus.large-message-client.message-size-threshold` | `262144` (256 KB) | Size threshold for offloading |
| `azure.servicebus.large-message-client.always-through-blob` | `false` | Force all messages through blob |
| `azure.servicebus.large-message-client.cleanup-blob-on-delete` | `true` | Auto-delete blob on message delete |
| `azure.servicebus.large-message-client.blob-key-prefix` | `""` | Prefix for blob names |
| `azure.servicebus.large-message-client.sas-enabled` | `false` | Enable SAS URI generation |
| `azure.servicebus.large-message-client.sas-token-validation-time` | `P7D` | SAS token validity (ISO-8601 duration) |
| `azure.servicebus.large-message-client.message-property-for-blob-sas-uri` | `"$attachment.sas.uri"` | Message property for SAS URI |
| `azure.servicebus.large-message-client.receive-only-mode` | `false` | Enable receive-only mode |
| `azure.servicebus.large-message-client.retry-max-attempts` | `3` | Maximum retry attempts |
| `azure.servicebus.large-message-client.retry-backoff-millis` | `1000` | Initial backoff delay (ms) |
| `azure.servicebus.large-message-client.retry-backoff-multiplier` | `2.0` | Backoff multiplier |
| `azure.servicebus.large-message-client.retry-max-backoff-millis` | `30000` | Maximum backoff delay cap (ms) |
| `azure.servicebus.large-message-client.dead-letter-on-failure` | `true` | Dead-letter messages on failure |
| `azure.servicebus.large-message-client.dead-letter-reason` | `"ProcessingFailure"` | Default dead-letter reason |
| `azure.servicebus.large-message-client.max-delivery-count` | `10` | Informational only |

## Utilities Reference

### Application Property Validation

`ApplicationPropertyValidator` enforces safe limits on custom message application properties:

- Rejects reserved property names used internally by the library
- Enforces a maximum count of user-defined properties (default: 9)
- Limits total properties size to 64 KB

```java
Map<String, Object> props = Map.of("tenantId", "abc", "priority", "high");
ApplicationPropertyValidator.validate(props, config.getMaxAllowedProperties());
```

### Blob Key Prefix Validation

`BlobKeyPrefixValidator` validates blob key prefixes used for organizing payloads:

- Maximum length: 988 characters
- Allowed characters: alphanumeric, dots, underscores, slashes, and hyphens

### Duplicate Detection

`DuplicateDetectionHelper` computes SHA-256 content hashes for deduplication:

```java
String hash = DuplicateDetectionHelper.computeContentHash(messageBody);
// Returns a Base64-encoded SHA-256 hash suitable for use as a deduplication ID
```

### Encryption Configuration

`EncryptionConfiguration` provides encryption scope and customer-provided key support for blob uploads. See the [README](README.md) for full details.

## Testing

### Running Unit Tests

```bash
mvn test
```

### Running Local Integration Tests

These tests use mocks and Azurite — no Azure credentials required:

```bash
mvn verify -P integration-test-local
```

### Running Azure Integration Tests

Requires live Azure Service Bus and Storage credentials:

```bash
export AZURE_SERVICEBUS_CONNECTION_STRING="Endpoint=sb://..."
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
mvn verify -P integration-test-azure
```

### Running All Integration Tests

```bash
mvn verify -P integration-test
```

## Conclusion
This guide covers the key features and usage patterns of the Azure Service Bus Large Message Client. For more details, refer to the [README](README.md) and the example application in the repository.
