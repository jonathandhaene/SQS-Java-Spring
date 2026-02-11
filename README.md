# Azure Service Bus Large Message Client for Java Spring Boot

> ⚠️ **DISCLAIMER: FOR ILLUSTRATION PURPOSES ONLY**
> 
> This project is a **proof-of-concept / illustrative example** and is **NOT production-ready code**.
> It is provided "as is" without warranty of any kind. The author(s) accept **no responsibility or liability**
> for any issues, damages, or consequences arising from the use of this software.
> **Do not use this code in a production environment** without thorough review, testing, and modification.

An illustrative example library that demonstrates how to transparently offload large message payloads to Azure Blob Storage when they exceed the configured threshold, providing a seamless experience for working with messages of any size.

## Features

### Core Capabilities
- **Automatic Payload Offloading**: Transparently stores large messages in Azure Blob Storage and sends a reference pointer via Service Bus
- **Seamless Message Resolution**: Automatically retrieves and resolves blob-stored payloads when receiving messages
- **Configurable Size Threshold**: Set custom thresholds for when messages should be offloaded (default: 256 KB)
- **Batch Operations**: Efficiently send and receive multiple messages with automatic batch splitting
- **Spring Boot Auto-Configuration**: Simple integration with Spring Boot applications

### Advanced Features
- **SAS URI Support**: Generate Shared Access Signature URIs for secure blob access without storage credentials
- **Receive-Only Mode**: Download blob payloads using only SAS URIs (no storage account credentials needed)
- **Custom Blob Naming**: Implement custom blob naming strategies (e.g., `{tenantId}/{messageId}`)
- **Custom Body Replacement**: Control what the message body becomes after blob offloading
- **Dynamic Connection Strings**: Integrate with Key Vault or other dynamic credential sources
- **Flexible Size Criteria**: Customize message offloading logic beyond simple size thresholds

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
    <groupId>com.azure.servicebus.largemessage</groupId>
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
    large-message-client:
      message-size-threshold: 262144    # 256 KB
      always-through-blob: false
      cleanup-blob-on-delete: true
  storage:
    connection-string: ${AZURE_STORAGE_CONNECTION_STRING}
    container-name: ${AZURE_STORAGE_CONTAINER_NAME:large-messages}
```

### Basic Usage

```java
@Autowired
private AzureServiceBusLargeMessageClient client;

// Send a message - automatically offloaded if too large
client.sendMessage("Your message content here");

// Receive messages - automatically resolved from blob storage
List<LargeServiceBusMessage> messages = client.receiveMessages(10);
for (LargeServiceBusMessage message : messages) {
    String body = message.getBody();
    
    // Clean up blob payload if needed
    if (message.isPayloadFromBlob()) {
        client.deletePayload(message);
    }
}
```

## Documentation

For detailed usage examples, configuration options, and advanced features, see the [Migration Guide](MIGRATION_GUIDE.md).

## Contributing

This is an illustrative project. Contributions are welcome but please note that no guarantees 
are made about maintenance or support. If you wish to use this in production, we recommend 
forking and adapting it to your specific needs with proper testing and security review.

## License & Disclaimer

This project is licensed under the MIT License with an additional disclaimer — see the [LICENSE](LICENSE) file.

**This code is for illustration purposes only and is not production-ready. The author(s) accept no responsibility for its use.**

## Disclaimer

> ⚠️ **THIS SOFTWARE IS PROVIDED FOR ILLUSTRATION AND EDUCATIONAL PURPOSES ONLY.**
>
> The author(s) of this project accept **NO responsibility or liability** for any issues, damages, 
> data loss, security vulnerabilities, or other consequences arising from the use of this software.
> This code is **NOT production-ready** and should **NOT** be used in any production environment 
> without thorough review, testing, hardening, and modification.
>
> **USE ENTIRELY AT YOUR OWN RISK.**