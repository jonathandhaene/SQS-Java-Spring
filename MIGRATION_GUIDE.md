# Guide: Transitioning from AWS SQS Extended Client to Azure Service Bus Extended Client

## Introduction
This migration guide provides specific instructions and examples for transitioning from the AWS SQS Extended Client to the Azure Service Bus Extended Client. It addresses API compatibility checks, configuration changes, and key architectural differences such as payload offloading, retries, and error handling.

## API Compatibility Checks
Before you start the migration, it's important to ensure that the APIs you are using are compatible:
1. **Message Structure**: Validate that the message structure used in AWS SQS can be mapped to Azure Service Bus messages.
2. **Message Attributes**: Review the attributes supported by both services and update your code to handle any differences.

## Configuration Changes
Update your configuration settings based on the following:
1. **Connection Strings**: Change the connection string format to match Azure Service Bus's requirements.
   
   - **AWS SQS Connection Example**:
     ```java
     String awsSqsUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue";
     ```
   - **Azure Service Bus Connection Example**:
     ```java
     String azureServiceBusConnectionString = "Endpoint=sb://<your_namespace>.servicebus.windows.net/;SharedAccessKeyName=<key_name>;SharedAccessKey=<key>";
     ```

2. **Queue Names**: Update the naming conventions if necessary, as Azure Service Bus may have different naming restrictions.

## Key Architectural Differences
### 1. Payload Offloading
- **AWS SQS** allows offloading large payloads using Amazon S3. 
- **Azure Service Bus** supports similar features using Azure Blob Storage, which can be implemented with the following code:
  ```java
  // Example for Azure Blob
  BlobClient blobClient = new BlobClientBuilder() 
      .connectionString(azureBlobConnectionString) 
      .containerName("yourContainer") 
      .blobName("yourBlob") 
      .buildClient();
  blobClient.upload(new ByteArrayInputStream(messageBytes), messageBytes.length);
  ```

### 2. Retries
- AWS SQS has built-in retry policies which can be configured via visibility timeouts.
- Azure Service Bus uses a different mechanism:
  - Implement retry policies through message handlers and middleware.
  ```java
  // Example retry mechanism
  receiveMessages();
  ``` 

### 3. Error Handling
- Handling errors in SQS involves simply using dead letter queues.
- In Azure Service Bus, you can configure dead letter messages, and you may also need to handle them explicitly:
  ```java
  // Example of handling dead letter messages
  serviceBusReceiver.receiveMessages().thenAccept(receivedMessage -> {
      // Process message
  });
  ```

## Java Code Examples
### Example of Sending a Message
**AWS SQS**:  
```java
SendMessageRequest send_msg_request = SendMessageRequest.builder()
    .queueUrl(queueUrl)
    .messageBody(message)
    .build();
SendMessageResponse send_msg_response = sqsClient.sendMessage(send_msg_request);
```

**Azure Service Bus**:  
```java
Sender sender = client.createSender(queueName);
Sender.sendMessage(new ServiceBusMessage(message));
```

### Example of Receiving a Message
**AWS SQS**:  
```java
ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
    .queueUrl(queueUrl)
    .build();
List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
```

**Azure Service Bus**:
```java
Receiver receiver = client.createReceiver(queueName);
Receiver.receiveMessages().forEach(message -> {
    // Process the message
});
```

## Conclusion
Migrating from AWS SQS to Azure Service Bus requires careful consideration of the differences in APIs, configurations, and architectural approaches. By following this guide and utilizing the provided examples, you can make the transition as smooth as possible.