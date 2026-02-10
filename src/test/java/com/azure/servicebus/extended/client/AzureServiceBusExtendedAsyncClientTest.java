package com.azure.servicebus.extended.client;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AzureServiceBusExtendedAsyncClient.
 */
class AzureServiceBusExtendedAsyncClientTest {

    private ServiceBusSenderAsyncClient mockSenderClient;
    private ServiceBusReceiverAsyncClient mockReceiverClient;
    private BlobPayloadStore mockPayloadStore;
    private ExtendedClientConfiguration config;
    private AzureServiceBusExtendedAsyncClient client;

    @BeforeEach
    void setUp() {
        mockSenderClient = mock(ServiceBusSenderAsyncClient.class);
        mockReceiverClient = mock(ServiceBusReceiverAsyncClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
        
        config = new ExtendedClientConfiguration();
        config.setMessageSizeThreshold(1024); // 1 KB for testing
        config.setAlwaysThroughBlob(false);
        config.setCleanupBlobOnDelete(true);
        config.setBlobKeyPrefix("");

        client = new AzureServiceBusExtendedAsyncClient(
                mockSenderClient,
                mockReceiverClient,
                mockPayloadStore,
                config
        );
    }

    @Test
    void testAsyncSendSmallMessage() {
        // Arrange
        String smallMessage = "Small test message";
        when(mockSenderClient.sendMessage(any(ServiceBusMessage.class))).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessage(smallMessage))
                .expectComplete()
                .verify();

        verify(mockSenderClient, times(1)).sendMessage(any(ServiceBusMessage.class));
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testAsyncSendLargeMessage() {
        // Arrange
        String largeMessage = generateMessage(2048); // 2 KB, exceeds threshold
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);
        when(mockSenderClient.sendMessage(any(ServiceBusMessage.class))).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessage(largeMessage))
                .expectComplete()
                .verify();

        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessage(any(ServiceBusMessage.class));
    }

    @Test
    void testAsyncSendMessageBatch() {
        // Arrange
        List<String> messages = Arrays.asList("Message 1", "Message 2", "Message 3");
        when(mockSenderClient.sendMessages(anyIterable())).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessageBatch(messages))
                .expectComplete()
                .verify();

        verify(mockSenderClient, times(1)).sendMessages(anyIterable());
    }

    @Test
    void testAsyncSendMessageBatch_mixedSizes() {
        // Arrange
        String smallMessage = "Small";
        String largeMessage = generateMessage(2048);
        List<String> messages = Arrays.asList(smallMessage, largeMessage);
        
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);
        when(mockSenderClient.sendMessages(anyIterable())).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessageBatch(messages))
                .expectComplete()
                .verify();

        // Large message should be offloaded
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessages(anyIterable());
    }

    @Test
    void testAsyncReceiveMessages() {
        // Arrange
        ServiceBusReceivedMessage mockMessage = mock(ServiceBusReceivedMessage.class);
        when(mockMessage.getMessageId()).thenReturn("msg-1");
        when(mockMessage.getBody()).thenReturn(BinaryData.fromString("test body"));
        when(mockMessage.getApplicationProperties()).thenReturn(new HashMap<>());
        
        when(mockReceiverClient.receiveMessages())
                .thenReturn(Flux.just(mockMessage));

        // Act & Assert
        StepVerifier.create(client.receiveMessages(1))
                .assertNext(extendedMessage -> {
                    assertEquals("msg-1", extendedMessage.getMessageId());
                    assertEquals("test body", extendedMessage.getBody());
                    assertFalse(extendedMessage.isPayloadFromBlob());
                })
                .expectComplete()
                .verify();
    }

    @Test
    void testAsyncDeletePayload() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id", "body", new HashMap<>(), true, pointer);

        // Act & Assert
        StepVerifier.create(client.deletePayload(message))
                .expectComplete()
                .verify();

        verify(mockPayloadStore, times(1)).deletePayload(pointer);
    }

    @Test
    void testAsyncDeletePayloadBatch() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob1");
        BlobPointer pointer2 = new BlobPointer("container", "blob2");
        
        ExtendedServiceBusMessage msg1 = new ExtendedServiceBusMessage(
                "msg-1", "body1", new HashMap<>(), true, pointer1);
        ExtendedServiceBusMessage msg2 = new ExtendedServiceBusMessage(
                "msg-2", "body2", new HashMap<>(), true, pointer2);
        
        List<ExtendedServiceBusMessage> messages = Arrays.asList(msg1, msg2);

        // Act & Assert
        StepVerifier.create(client.deletePayloadBatch(messages))
                .expectComplete()
                .verify();

        verify(mockPayloadStore, times(1)).deletePayload(pointer1);
        verify(mockPayloadStore, times(1)).deletePayload(pointer2);
    }

    @Test
    void testAsyncRenewMessageLock() {
        // Arrange
        ServiceBusReceivedMessage mockMessage = mock(ServiceBusReceivedMessage.class);
        when(mockMessage.getMessageId()).thenReturn("msg-1");
        when(mockReceiverClient.renewMessageLock(mockMessage))
                .thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.renewMessageLock(mockMessage))
                .expectComplete()
                .verify();

        verify(mockReceiverClient, times(1)).renewMessageLock(mockMessage);
    }

    @Test
    void testAsyncRenewMessageLockBatch() {
        // Arrange
        ServiceBusReceivedMessage mockMessage1 = mock(ServiceBusReceivedMessage.class);
        ServiceBusReceivedMessage mockMessage2 = mock(ServiceBusReceivedMessage.class);
        when(mockMessage1.getMessageId()).thenReturn("msg-1");
        when(mockMessage2.getMessageId()).thenReturn("msg-2");
        
        when(mockReceiverClient.renewMessageLock(any(ServiceBusReceivedMessage.class)))
                .thenReturn(Mono.empty());
        
        List<ServiceBusReceivedMessage> messages = Arrays.asList(mockMessage1, mockMessage2);

        // Act & Assert
        StepVerifier.create(client.renewMessageLockBatch(messages))
                .expectComplete()
                .verify();

        verify(mockReceiverClient, times(2)).renewMessageLock(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void testAsyncPayloadSupportDisabled() {
        // Arrange
        config.setPayloadSupportEnabled(false);
        String largeMessage = generateMessage(5000);
        when(mockSenderClient.sendMessage(any(ServiceBusMessage.class))).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessage(largeMessage))
                .expectComplete()
                .verify();

        // Message should be sent directly, not offloaded
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testAsyncUserAgentTracking() {
        // Arrange
        String messageBody = "Test message";
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        when(mockSenderClient.sendMessage(messageCaptor.capture())).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(client.sendMessage(messageBody))
                .expectComplete()
                .verify();

        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT));
    }

    /**
     * Helper method to generate a message of specified size.
     */
    private String generateMessage(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
