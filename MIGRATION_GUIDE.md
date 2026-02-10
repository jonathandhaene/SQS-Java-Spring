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
  BlobClient blobClient = new BlobClientBuilder() 
      .connectionString(azureBlobConnectionString) 
      .containerName("yourContainer") 
      .blobName("yourBlobName") 
      .buildClient();
  blobClient.upload(new ByteArrayInputStream(messageBytes), messageBytes.length);
  ```

### 2. Retries
- AWS SQS has built-in retry policies with configurable visibility timeouts.
- Azure Service Bus retries can be configured programmatically or via the SDK options:
  ```java
  ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
      .connectionString(azureServiceBusConnectionString)
      .retryOptions(new AmqpRetryOptions()
          .setMaxRetries(3)
          .setMode(AmqpRetryMode.EXPONENTIAL)
          .setMaxDelay(Duration.ofSeconds(30))
      );
  ``` 

### 3. Error Handling
- Handling errors in **AWS SQS** involves interacting with Dead Letter Queues (DLQs).
- In **Azure Service Bus**, DLQs are built-in and can be used seamlessly. Example:
  ```java
  ServiceBusProcessorClient processor = new ServiceBusClientBuilder()
      .processor()
      .connectionString(azureServiceBusConnectionString)
      .queueName("my-queue")
      .processMessage(context -> {
          System.out.println("Processing message: " + context.getMessage().getBody().toString());
      })
      .processError(context -> {
          System.err.println("Error occurred: " + context.getException());
      })
      .buildProcessorClient();
  processor.start();
  ```

## Java Code Examples
### Example of Sending a Message
**AWS SQS**:
```java
SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
    .queueUrl(queueUrl)
    .messageBody("Sample message")
    .build();
SendMessageResponse response = sqsClient.sendMessage(sendMsgRequest);
System.out.println("Message ID: " + response.messageId());
```

**Azure Service Bus**:
```java
ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
    .connectionString(azureServiceBusConnectionString)
    .sender()
    .queueName("queue-name")
    .buildClient();

senderClient.sendMessage(new ServiceBusMessage("Sample message"));
System.out.println("Message sent successfully.");
```

### Example of Receiving Messages
**AWS SQS**:
```java
ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
    .queueUrl(queueUrl)
    .maxNumberOfMessages(10)
    .build();
List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
for (Message message : messages) {
    System.out.println("Message: " + message.body());
}
```

**Azure Service Bus**:
```java
ServiceBusReceiverClient receiverClient = new ServiceBusClientBuilder()
    .connectionString(azureServiceBusConnectionString)
    .receiver()
    .queueName("queue-name")
    .buildClient();

IterableStream<ServiceBusReceivedMessage> messages = receiverClient.receiveMessages(10);
for (ServiceBusReceivedMessage message : messages) {
    System.out.println("Message: " + message.getBody().toString());
    receiverClient.complete(message);
}
```

## Conclusion
Migrating from AWS SQS to Azure Service Bus requires careful consideration of the differences in APIs, configurations, and architectural approaches. By following this guide and utilizing the provided examples, you can make the transition as smooth as possible.