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

## Key Features

### ✅ Automatic Payload Offloading
- Automatically stores large payloads in blob storage when they exceed the configurable threshold
- Configurable size threshold (default 256 KB)
- Optional "always through blob" mode to force all messages to use blob storage

### ✅ Async Client Support
- Reactive programming with Project Reactor (Mono/Flux)
- `AzureServiceBusExtendedAsyncClient` for non-blocking operations
- Supports async send, receive, and batch operations

### ✅ Batch Operations
- **Batch Send**: Send multiple messages in a single operation with `sendMessageBatch()`
- **Batch Delete**: Delete multiple blob payloads efficiently with `deletePayloadBatch()`
- **Batch Lock Renewal**: Renew message locks in bulk for extended processing time

### ✅ Encryption Support
- **Encryption Scope**: Server-side encryption with Azure-managed encryption scopes
- **Customer-Provided Key (CPK)**: Client-side encryption with your own keys
- Configurable encryption for blob payloads

### ✅ Access Tier Configuration
- Configure blob access tiers (Hot, Cool, Archive) for cost optimization
- Control storage costs based on access patterns
- Set default tier for all offloaded payloads

### ✅ Legacy Attribute Name Support
- Backward compatibility with AWS SQS Extended Client attribute names
- Toggle between legacy and modern attribute naming conventions
- Seamless migration from AWS to Azure

### ✅ Payload Support Toggle
- Ability to enable/disable payload offloading at runtime
- `payload-support-enabled` configuration for flexible deployment scenarios
- Useful for testing or gradual rollout

### ✅ User-Agent Tracking
- Custom user-agent header for blob operations
- Helps identify and track library usage in Azure logs
- Includes library version information

### ✅ Blob Key Prefix Validation
- Validates blob key prefixes to ensure Azure Blob Storage compatibility
- Prevents invalid characters and naming conflicts
- Automatic sanitization of blob names

### ✅ Property Validation
- Configurable maximum allowed properties per message
- Prevents exceeding Azure Service Bus limits (default: 64 properties)
- Clear error messages for validation failures

### ✅ IgnorePayloadNotFound Option
- Gracefully handle missing blob payloads with `ignore-payload-not-found`
- Prevents failures when blobs are already deleted or expired
- Useful for retry scenarios and cleanup edge cases

### ✅ Message Lock Renewal
- `renewMessageLock()` method to extend message processing time
- Prevents message timeout during long-running operations
- Similar to AWS SQS `changeMessageVisibility`

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
| `AmazonSQSExtendedAsyncClient` | `AzureServiceBusExtendedAsyncClient` | Async client |
| `S3BackedPayloadStore` | `BlobPayloadStore` | Storage layer |
| `PayloadS3Pointer` | `BlobPointer` | Payload reference |
| `changeMessageVisibility` | `renewMessageLock` | Lock renewal |
| `sendMessageBatch` | `sendMessageBatch` | Batch send |
| `deleteMessageBatch` | `deletePayloadBatch` | Batch delete |
| `ServerSideEncryptionStrategy` | `EncryptionConfiguration` | Encryption |

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
    ignore-payload-not-found: false
    use-legacy-reserved-attribute-name: false
    payload-support-enabled: true
    blob-access-tier: "Hot"           # Hot, Cool, or Archive
    max-allowed-properties: 64
    encryption:
      encryption-scope: ""            # Optional Azure encryption scope
      customer-provided-key: ""       # Optional customer-provided key (base64)
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

### 4. Use Async Client

```java
@Autowired
private AzureServiceBusExtendedAsyncClient asyncClient;

// Send message asynchronously
Mono<Void> sendMono = asyncClient.sendMessage("Hello async world");
sendMono.subscribe();

// Receive messages reactively
Flux<ExtendedServiceBusMessage> messages = asyncClient.receiveMessages(10);
messages.subscribe(message -> {
    System.out.println("Received: " + message.getBody());
});

// With error handling
asyncClient.sendMessage(largeMessage)
    .doOnSuccess(v -> System.out.println("Sent successfully"))
    .doOnError(e -> System.err.println("Error: " + e.getMessage()))
    .subscribe();
```

### 5. Batch Operations

```java
// Batch send messages
List<String> messages = Arrays.asList(
    "Message 1",
    "Message 2", 
    "Message 3"
);
client.sendMessageBatch(messages);

// Batch send with properties
List<Map<String, Object>> messagesWithProps = new ArrayList<>();
messages.forEach(msg -> {
    Map<String, Object> props = new HashMap<>();
    props.put("timestamp", System.currentTimeMillis());
    messagesWithProps.add(props);
});
client.sendMessageBatch(messages, messagesWithProps);

// Batch delete payloads
List<ExtendedServiceBusMessage> receivedMessages = client.receiveMessages(10);
client.deletePayloadBatch(receivedMessages);
```

### 6. Renew Message Locks

```java
// Receive message
ExtendedServiceBusMessage message = client.receiveMessages(1).get(0);

// Process for extended period
try {
    // Long-running operation...
    Thread.sleep(30000);
    
    // Renew lock to prevent timeout
    client.renewMessageLock(message);
    
    // Continue processing...
    processMessage(message);
    
} finally {
    // Clean up
    if (message.isPayloadFromBlob()) {
        client.deletePayload(message);
    }
}
```

### 7. Disable Payload Support

```java
// In application.yml
azure:
  extended-client:
    payload-support-enabled: false  # Disable payload offloading

// Or programmatically
ExtendedClientConfiguration config = new ExtendedClientConfiguration();
config.setPayloadSupportEnabled(false);

// Now all messages go directly through Service Bus (no blob storage)
client.sendMessage(message); // Sent directly regardless of size
```

### 8. Receive Messages

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

### 9. Process Messages (Event-Driven)

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

## Configuration Reference

### Core Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `azure.servicebus.connection-string` | *required* | Service Bus connection string |
| `azure.servicebus.queue-name` | `my-queue` | Queue name |
| `azure.storage.connection-string` | *required* | Blob Storage connection string |
| `azure.storage.container-name` | `large-messages` | Container for large payloads |

### Extended Client Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `azure.extended-client.message-size-threshold` | `262144` (256 KB) | Size threshold for offloading messages to blob storage |
| `azure.extended-client.always-through-blob` | `false` | Force all messages through blob storage regardless of size |
| `azure.extended-client.cleanup-blob-on-delete` | `true` | Auto-delete blob payload when message is deleted |
| `azure.extended-client.blob-key-prefix` | `""` | Prefix for blob names (e.g., "messages/" or "prod/") |
| `azure.extended-client.ignore-payload-not-found` | `false` | Gracefully handle missing blob payloads without errors |
| `azure.extended-client.use-legacy-reserved-attribute-name` | `false` | Use AWS SQS Extended Client compatible attribute names |
| `azure.extended-client.payload-support-enabled` | `true` | Enable/disable payload offloading feature |
| `azure.extended-client.blob-access-tier` | `"Hot"` | Blob storage access tier: Hot, Cool, or Archive |
| `azure.extended-client.max-allowed-properties` | `64` | Maximum number of properties allowed per message |

### Encryption Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `azure.extended-client.encryption.encryption-scope` | `""` | Azure-managed encryption scope for server-side encryption |
| `azure.extended-client.encryption.customer-provided-key` | `""` | Base64-encoded customer-provided key for client-side encryption |

### Configuration Examples

**Basic Configuration:**
```yaml
azure:
  extended-client:
    message-size-threshold: 262144
    cleanup-blob-on-delete: true
```

**With Encryption:**
```yaml
azure:
  extended-client:
    encryption:
      encryption-scope: "my-encryption-scope"
```

**Cost Optimization:**
```yaml
azure:
  extended-client:
    blob-access-tier: "Cool"  # Reduce storage costs for infrequent access
```

**Legacy Compatibility:**
```yaml
azure:
  extended-client:
    use-legacy-reserved-attribute-name: true  # AWS SQS compatibility
```

**Development/Testing:**
```yaml
azure:
  extended-client:
    payload-support-enabled: false  # Disable offloading for testing
    ignore-payload-not-found: true  # Handle missing blobs gracefully
```

## Architecture Components

### Core Classes

- **`AzureServiceBusExtendedClient`**: Main client for sending/receiving messages
- **`AzureServiceBusExtendedAsyncClient`**: Async client with Mono/Flux support
- **`BlobPayloadStore`**: Handles blob storage operations
- **`BlobPointer`**: JSON-serializable pointer to blob payloads
- **`ExtendedServiceBusMessage`**: Wrapper for received messages with resolved payloads
- **`ExtendedClientConfiguration`**: Configuration properties
- **`EncryptionConfiguration`**: Encryption settings (scope and CPK)
- **`AzureExtendedClientAutoConfiguration`**: Spring Boot auto-configuration

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

## Thread Safety

> ⚠️ Note: Thread safety has not been rigorously tested. This code is for illustrative purposes only.

The library is designed to be thread-safe:
- `AzureServiceBusExtendedClient` can be safely used from multiple threads
- Blob storage operations are atomic
- Service Bus clients handle concurrency internally

## Error Handling

> ⚠️ Note: Error handling in this project is minimal and for demonstration only. Production use would require comprehensive error handling, retry logic, circuit breakers, and proper monitoring.

- Storage failures throw `RuntimeException` with detailed error messages
- Blob cleanup failures are logged but don't fail message processing
- 404 errors on blob deletion are handled gracefully
- All errors are logged with SLF4J

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