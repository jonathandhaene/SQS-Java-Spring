# Azure Service Bus Large Message Client for Java Spring Boot

A powerful Java Spring Boot library for handling large messages with Azure Service Bus. This library automatically offloads large message payloads to Azure Blob Storage when they exceed the configured threshold, providing a seamless experience for working with messages of any size.

## Features

### Core Capabilities
- **Automatic Payload Offloading**: Transparently stores large messages in Azure Blob Storage and sends a reference pointer via Service Bus
- **Seamless Message Resolution**: Automatically retrieves and resolves blob-stored payloads when receiving messages
- **Configurable Size Threshold**: Set custom thresholds for when messages should be offloaded (default: 256 KB)
- **Batch Operations**: Efficiently send and receive multiple messages with automatic batch splitting
- **Spring Boot Auto-Configuration**: Simple integration with Spring Boot applications

### Azure-Native Features
This library leverages unique Azure Service Bus capabilities:
- **Scheduled Messages**: Schedule messages for future delivery with built-in support
- **Message Sessions**: Support for ordered, grouped message processing using session IDs
- **Message Deferral**: Defer messages for later retrieval by sequence number
- **Message Lock Renewal**: Extend processing time for long-running operations
- **Native Dead Letter Queue**: Rich metadata support with automatic dead-lettering on failure
- **Duplicate Detection**: Built-in duplicate message detection
- **Application Property Validation**: Comprehensive validation of custom message properties
- **Retry Logic**: Configurable exponential backoff with jitter for transient failures

## Installation

Add the dependency to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>com.azure.servicebus.extended</groupId>
    <artifactId>azure-servicebus-large-message-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Quick Start

### Configuration

Configure your `application.yml`:

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
```

### Basic Usage

```java
@Autowired
private AzureServiceBusExtendedClient client;

// Send a message - automatically offloaded if too large
client.sendMessage("Your message content here");

// Receive messages - automatically resolved from blob storage
List<ExtendedServiceBusMessage> messages = client.receiveMessages(10);
for (ExtendedServiceBusMessage message : messages) {
    String body = message.getBody();
    
    // Clean up blob payload if needed
    if (message.isPayloadFromBlob()) {
        client.deletePayload(message);
    }
}
```

## Documentation

For detailed usage examples, configuration options, and advanced features, see the [Migration Guide](MIGRATION_GUIDE.md).

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.