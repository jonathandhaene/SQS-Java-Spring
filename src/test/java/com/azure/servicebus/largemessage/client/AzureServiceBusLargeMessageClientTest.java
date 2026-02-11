/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.client;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AzureServiceBusLargeMessageClient.
 */
class AzureServiceBusLargeMessageClientTest {

    private ServiceBusSenderClient mockSenderClient;
    private ServiceBusReceiverClient mockReceiverClient;
    private BlobPayloadStore mockPayloadStore;
    private LargeMessageClientConfiguration config;
    private AzureServiceBusLargeMessageClient client;

    @BeforeEach
    void setUp() {
        mockSenderClient = mock(ServiceBusSenderClient.class);
        mockReceiverClient = mock(ServiceBusReceiverClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
        
        config = new LargeMessageClientConfiguration();
        config.setMessageSizeThreshold(1024); // 1 KB for testing
        config.setAlwaysThroughBlob(false);
        config.setCleanupBlobOnDelete(true);
        config.setBlobKeyPrefix("");
        config.setUseLegacyReservedAttributeName(false); // Use modern name for tests

        client = new AzureServiceBusLargeMessageClient(
                mockSenderClient,
                mockReceiverClient,
                mockPayloadStore,
                config
        );
    }

    @Test
    void testSmallMessage_sentDirectly() {
        // Arrange
        String smallMessage = "Small test message";
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals(smallMessage, capturedMessage.getBody().toString());
        assertFalse(capturedMessage.getApplicationProperties().containsKey(
                LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
        
        // Verify no blob interaction
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testLargeMessage_offloadedToBlob() {
        // Arrange
        String largeMessage = generateMessage(2048); // 2 KB, exceeds threshold
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        String messageBody = capturedMessage.getBody().toString();
        
        // Verify message body contains blob pointer JSON
        assertTrue(messageBody.contains("containerName"));
        assertTrue(messageBody.contains("blobName"));
        
        // Verify application properties
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                LargeMessageClientConfiguration.RESERVED_ATTRIBUTE_NAME));
    }

    @Test
    void testAlwaysThroughBlob_smallMessageStillOffloaded() {
        // Arrange
        config.setAlwaysThroughBlob(true);
        String smallMessage = "Small message";
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(smallMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(smallMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
    }

    @Test
    void testDeletePayload_cleansUpBlob() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true, // from blob
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, times(1)).deletePayload(pointer);
    }

    @Test
    void testDeletePayload_skipsWhenNotFromBlob() {
        // Arrange
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                false, // not from blob
                null
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testDeletePayload_skipsWhenCleanupDisabled() {
        // Arrange
        config.setCleanupBlobOnDelete(false);
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true,
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testSendMessage_withApplicationProperties() {
        // Arrange
        String messageBody = "Test message";
        Map<String, Object> customProps = new HashMap<>();
        customProps.put("customKey", "customValue");
        customProps.put("timestamp", 12345L);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(messageBody, customProps);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals("customValue", capturedMessage.getApplicationProperties().get("customKey"));
        assertEquals(12345L, capturedMessage.getApplicationProperties().get("timestamp"));
    }

    @Test
    void testBlobPointer_serializationRoundTrip() {
        // Arrange
        BlobPointer original = new BlobPointer("my-container", "my-blob-123");

        // Act
        String json = original.toJson();
        BlobPointer deserialized = BlobPointer.fromJson(json);

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("my-container"));
        assertTrue(json.contains("my-blob-123"));
        assertEquals(original, deserialized);
        assertEquals(original.getContainerName(), deserialized.getContainerName());
        assertEquals(original.getBlobName(), deserialized.getBlobName());
    }

    @Test
    void testBlobPointer_equality() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob");
        BlobPointer pointer2 = new BlobPointer("container", "blob");
        BlobPointer pointer3 = new BlobPointer("container", "different-blob");

        // Assert
        assertEquals(pointer1, pointer2);
        assertNotEquals(pointer1, pointer3);
        assertEquals(pointer1.hashCode(), pointer2.hashCode());
    }

    @Test
    void testBlobPointer_toString() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");

        // Act
        String string = pointer.toString();

        // Assert
        assertTrue(string.contains("test-container"));
        assertTrue(string.contains("test-blob"));
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

    // ========== Retry Logic Tests ==========

    @Test
    void testSendMessage_retriesOnFailure() {
        // Arrange
        String message = "Test message";
        
        // Mock sender to fail twice, then succeed
        doThrow(new RuntimeException("Transient failure 1"))
            .doThrow(new RuntimeException("Transient failure 2"))
            .doNothing()
            .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

        // Act
        client.sendMessage(message);

        // Assert
        verify(mockSenderClient, times(3)).sendMessage(any(ServiceBusMessage.class));
    }

    @Test
    void testSendMessage_failsAfterMaxRetries() {
        // Arrange
        String message = "Test message";
        
        // Mock sender to always fail
        doThrow(new RuntimeException("Persistent failure"))
            .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> client.sendMessage(message));
        
        // Should attempt 3 times (initial + 2 retries) based on default config
        verify(mockSenderClient, times(3)).sendMessage(any(ServiceBusMessage.class));
    }

    @Test
    void testLargeMessage_retriesBlobUpload() {
        // Arrange
        String largeMessage = generateMessage(2048); // Exceeds threshold
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        // Mock blob store to fail twice, then succeed
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
            .thenThrow(new RuntimeException("Transient blob failure 1"))
            .thenThrow(new RuntimeException("Transient blob failure 2"))
            .thenReturn(expectedPointer);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockPayloadStore, times(3)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessage(any(ServiceBusMessage.class));
    }

    @Test
    void testDeletePayload_retriesOnTransientFailure() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true,
                pointer
        );
        
        // Mock delete to fail twice, then succeed
        doThrow(new RuntimeException("Transient delete failure 1"))
            .doThrow(new RuntimeException("Transient delete failure 2"))
            .doNothing()
            .when(mockPayloadStore).deletePayload(pointer);

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, times(3)).deletePayload(pointer);
    }

    // ========== Dead Letter Queue Tests ==========

    @Test
    void testConfiguration_dlqDefaults() {
        // Verify DLQ configuration has expected defaults
        assertTrue(config.isDeadLetterOnFailure());
        assertEquals("ProcessingFailure", config.getDeadLetterReason());
        assertEquals(10, config.getMaxDeliveryCount());
    }

    @Test
    void testLargeServiceBusMessage_includesDLQFields() {
        // Arrange & Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                false,
                null,
                "MaxDeliveryCountExceeded",
                "Message exceeded max delivery attempts",
                5
        );

        // Assert
        assertEquals("MaxDeliveryCountExceeded", message.getDeadLetterReason());
        assertEquals("Message exceeded max delivery attempts", message.getDeadLetterDescription());
        assertEquals(5, message.getDeliveryCount());
    }

    @Test
    void testLargeServiceBusMessage_backwardCompatibleConstructor() {
        // Arrange & Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                false,
                null
        );

        // Assert - should have null DLQ fields and 0 delivery count
        assertNull(message.getDeadLetterReason());
        assertNull(message.getDeadLetterDescription());
        assertEquals(0, message.getDeliveryCount());
    }

    @Test
    void testConfiguration_retryDefaults() {
        // Assert retry configuration defaults
        assertEquals(3, config.getRetryMaxAttempts());
        assertEquals(1000L, config.getRetryBackoffMillis());
        assertEquals(2.0, config.getRetryBackoffMultiplier());
        assertEquals(30000L, config.getRetryMaxBackoffMillis());
    }

    // ========== Send with Session ID Tests ==========

    @Test
    void testSendMessage_withSessionId() {
        String body = "session message";
        String sessionId = "session-42";
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendMessage(body, sessionId);

        verify(mockSenderClient).sendMessage(captor.capture());
        assertEquals(sessionId, captor.getValue().getSessionId());
    }

    @Test
    void testSendMessage_withSessionIdAndProperties() {
        String body = "session prop message";
        Map<String, Object> props = new HashMap<>();
        props.put("key", "value");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendMessage(body, "sess-1", props);

        verify(mockSenderClient).sendMessage(captor.capture());
        assertEquals("sess-1", captor.getValue().getSessionId());
        assertEquals("value", captor.getValue().getApplicationProperties().get("key"));
    }

    @Test
    void testSendLargeMessage_withSessionId_offloadsAndPreservesSession() {
        String largeBody = generateMessage(2048);
        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), eq(largeBody))).thenReturn(pointer);
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendMessage(largeBody, "session-large");

        verify(mockPayloadStore).storePayload(anyString(), eq(largeBody));
        verify(mockSenderClient).sendMessage(captor.capture());
        assertEquals("session-large", captor.getValue().getSessionId());
        assertEquals("true", captor.getValue().getApplicationProperties()
                .get(LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
    }

    // ========== SAS URI Generation Tests ==========

    @Test
    void testSendMessage_withSasEnabled_generatesSasUri() {
        config.setSasEnabled(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSenderClient, mockReceiverClient, mockPayloadStore, config);
        String largeBody = generateMessage(2048);
        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), eq(largeBody))).thenReturn(pointer);
        when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                .thenReturn("https://store.blob.core.windows.net/c/b?sv=...");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendMessage(largeBody);

        verify(mockSenderClient).sendMessage(captor.capture());
        String sasUri = (String) captor.getValue().getApplicationProperties()
                .get(config.getMessagePropertyForBlobSasUri());
        assertNotNull(sasUri);
        assertTrue(sasUri.contains("sv="));
    }

    @Test
    void testSendMessage_sasGenerationFails_continuesWithout() {
        config.setSasEnabled(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSenderClient, mockReceiverClient, mockPayloadStore, config);
        String largeBody = generateMessage(2048);
        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), eq(largeBody))).thenReturn(pointer);
        when(mockPayloadStore.generateSasUri(any(), any()))
                .thenThrow(new RuntimeException("SAS generation failed"));

        // Should not throw — SAS failures are swallowed
        assertDoesNotThrow(() -> client.sendMessage(largeBody));
        verify(mockSenderClient).sendMessage(any(ServiceBusMessage.class));
    }

    // ========== Duplicate Detection Tests ==========

    @Test
    void testSendMessage_duplicateDetectionEnabled_setsMessageId() {
        config.setEnableDuplicateDetectionId(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSenderClient, mockReceiverClient, mockPayloadStore, config);
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendMessage("test body");

        verify(mockSenderClient).sendMessage(captor.capture());
        assertNotNull(captor.getValue().getMessageId());
    }

    // ========== Nested Test Classes ==========

    @Nested
    @DisplayName("SendMessages (sequential)")
    class SendMessagesTests {

        @Test
        @DisplayName("Sends each message individually")
        void testSendMessages_sendsAll() {
            List<String> bodies = List.of("msg1", "msg2", "msg3");

            client.sendMessages(bodies);

            verify(mockSenderClient, times(3)).sendMessage(any(ServiceBusMessage.class));
        }

        @Test
        @DisplayName("Empty list results in no sends")
        void testSendMessages_emptyList() {
            client.sendMessages(new ArrayList<>());

            verify(mockSenderClient, never()).sendMessage(any(ServiceBusMessage.class));
        }
    }

    @Nested
    @DisplayName("SendMessageBatch")
    class SendMessageBatchTests {

        @Test
        @DisplayName("Batch sends all small messages without offloading")
        void testBatchSend_allSmall() {
            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(List.of("msg1", "msg2"));

            verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
            verify(mockSenderClient).sendMessages(mockBatch);
        }

        @Test
        @DisplayName("Batch offloads large messages and sends small directly")
        void testBatchSend_mixedSizes() {
            String small = "small";
            String large = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("c", "b");
            when(mockPayloadStore.storePayload(anyString(), eq(large))).thenReturn(pointer);

            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(List.of(small, large));

            verify(mockPayloadStore).storePayload(anyString(), eq(large));
            verify(mockSenderClient).sendMessages(mockBatch);
        }

        @Test
        @DisplayName("Empty list is a no-op")
        void testBatchSend_emptyList() {
            client.sendMessageBatch(new ArrayList<>());

            verify(mockSenderClient, never()).createMessageBatch();
        }

        @Test
        @DisplayName("Null list is a no-op")
        void testBatchSend_nullList() {
            client.sendMessageBatch(null);

            verify(mockSenderClient, never()).createMessageBatch();
        }

        @Test
        @DisplayName("Batch splits when batch is full")
        void testBatchSend_splitOnFull() {
            ServiceBusMessageBatch mockBatch1 = mock(ServiceBusMessageBatch.class);
            ServiceBusMessageBatch mockBatch2 = mock(ServiceBusMessageBatch.class);
            // First batch accepts 1 message then rejects
            when(mockBatch1.tryAddMessage(any(ServiceBusMessage.class)))
                    .thenReturn(true).thenReturn(false);
            when(mockBatch2.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch())
                    .thenReturn(mockBatch1).thenReturn(mockBatch2);

            client.sendMessageBatch(List.of("msg1", "msg2"));

            verify(mockSenderClient, times(2)).sendMessages(any(ServiceBusMessageBatch.class));
        }

        @Test
        @DisplayName("Single message too large for batch is sent individually")
        void testBatchSend_singleMessageTooLargeForBatch() {
            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            // First message doesn't fit in batch at all
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(false);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(List.of("oversized-message"));

            // Should be sent individually when it can't fit in a batch
            verify(mockSenderClient).sendMessage(any(ServiceBusMessage.class));
        }

        @Test
        @DisplayName("Batch send with application properties")
        void testBatchSend_withProperties() {
            Map<String, Object> props = new HashMap<>();
            props.put("key", "value");

            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(List.of("msg1"), props);

            verify(mockSenderClient).sendMessages(mockBatch);
        }

        @Test
        @DisplayName("Batch send with duplicate detection")
        void testBatchSend_withDuplicateDetection() {
            config.setEnableDuplicateDetectionId(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);

            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(List.of("msg1"));

            verify(mockSenderClient).sendMessages(mockBatch);
        }
    }

    @Nested
    @DisplayName("ReceiveMessages")
    class ReceiveMessagesTests {

        @Test
        @DisplayName("Receives and resolves blob-backed message")
        void testReceive_blobBacked() {
            String originalPayload = "original large payload";
            BlobPointer pointer = new BlobPointer("c", "blob-1");

            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-1");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
            when(mockMsg.getDeliveryCount()).thenReturn(1L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
            appProps.put(config.getReservedAttributeName(), 100);
            appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                    LargeMessageClientConfiguration.USER_AGENT_VALUE);
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn(originalPayload);
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            List<LargeServiceBusMessage> messages = client.receiveMessages(10);

            assertEquals(1, messages.size());
            assertEquals(originalPayload, messages.get(0).getBody());
            assertTrue(messages.get(0).isPayloadFromBlob());
            assertNotNull(messages.get(0).getBlobPointer());
        }

        @Test
        @DisplayName("Receives direct (non-blob) message")
        void testReceive_directMessage() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-direct");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("hello"));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                    LargeMessageClientConfiguration.USER_AGENT_VALUE);
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            List<LargeServiceBusMessage> messages = client.receiveMessages(10);

            assertEquals(1, messages.size());
            assertEquals("hello", messages.get(0).getBody());
            assertFalse(messages.get(0).isPayloadFromBlob());
        }

        @Test
        @DisplayName("Returns empty list when no messages available")
        void testReceive_empty() {
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of()));

            List<LargeServiceBusMessage> messages = client.receiveMessages(10);

            assertTrue(messages.isEmpty());
        }

        @Test
        @DisplayName("Strips user-agent header on receive")
        void testReceive_stripsUserAgent() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-ua");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body"));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, "test-agent");
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            List<LargeServiceBusMessage> messages = client.receiveMessages(1);

            assertFalse(messages.get(0).getApplicationProperties()
                    .containsKey(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT));
        }

        @Test
        @DisplayName("Strips blob marker and reserved attribute from received blob message")
        void testReceive_stripsInternalProperties() {
            BlobPointer pointer = new BlobPointer("c", "b");
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-strip");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
            appProps.put(config.getReservedAttributeName(), 100);
            appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT, "v");
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn("resolved");
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            List<LargeServiceBusMessage> messages = client.receiveMessages(1);

            assertFalse(messages.get(0).getApplicationProperties()
                    .containsKey(LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
            assertFalse(messages.get(0).getApplicationProperties()
                    .containsKey(config.getReservedAttributeName()));
        }

        @Test
        @DisplayName("Handles null payload from blob (ignorePayloadNotFound)")
        void testReceive_blobPayloadNull_setsEmptyBody() {
            BlobPointer pointer = new BlobPointer("c", "missing-blob");
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-null");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
            appProps.put(config.getReservedAttributeName(), 100);
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn(null);
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            List<LargeServiceBusMessage> messages = client.receiveMessages(1);

            assertEquals("", messages.get(0).getBody());
        }

        @Test
        @DisplayName("Receive failure wraps exception")
        void testReceive_failure() {
            when(mockReceiverClient.receiveMessages(anyInt(), any(Duration.class)))
                    .thenThrow(new RuntimeException("receive failed"));

            assertThrows(RuntimeException.class, () -> client.receiveMessages(10));
        }
    }

    @Nested
    @DisplayName("SendScheduledMessage")
    class SendScheduledMessageTests {

        @Test
        @DisplayName("Schedules a small message and returns sequence number")
        void testSchedule_smallMessage() {
            OffsetDateTime time = OffsetDateTime.now().plusMinutes(5);
            when(mockSenderClient.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                    .thenReturn(42L);

            Long seq = client.sendScheduledMessage("small body", time);

            assertEquals(42L, seq);
            verify(mockSenderClient).scheduleMessage(any(ServiceBusMessage.class), eq(time));
        }

        @Test
        @DisplayName("Schedules a large message with blob offloading")
        void testSchedule_largeMessage_offloaded() {
            String largeBody = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("c", "sched-blob");
            when(mockPayloadStore.storePayload(anyString(), eq(largeBody))).thenReturn(pointer);

            OffsetDateTime time = OffsetDateTime.now().plusMinutes(10);
            when(mockSenderClient.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                    .thenReturn(99L);

            Long seq = client.sendScheduledMessage(largeBody, time);

            assertEquals(99L, seq);
            verify(mockPayloadStore).storePayload(anyString(), eq(largeBody));
        }

        @Test
        @DisplayName("Schedules with custom application properties")
        void testSchedule_withProperties() {
            Map<String, Object> props = new HashMap<>();
            props.put("key", "value");
            OffsetDateTime time = OffsetDateTime.now().plusMinutes(1);
            when(mockSenderClient.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                    .thenReturn(1L);

            Long seq = client.sendScheduledMessage("body", time, props);

            assertNotNull(seq);
        }

        @Test
        @DisplayName("Schedule with duplicate detection sets message ID")
        void testSchedule_withDuplicateDetection() {
            config.setEnableDuplicateDetectionId(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);

            OffsetDateTime time = OffsetDateTime.now().plusMinutes(1);
            when(mockSenderClient.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                    .thenReturn(1L);

            ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
            client.sendScheduledMessage("body", time);

            verify(mockSenderClient).scheduleMessage(captor.capture(), eq(time));
            assertNotNull(captor.getValue().getMessageId());
        }
    }

    @Nested
    @DisplayName("DeferMessage")
    class DeferMessageTests {

        @Test
        @DisplayName("Defers a message successfully")
        void testDeferMessage_success() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-defer");
            when(mockMsg.getSequenceNumber()).thenReturn(123L);

            client.deferMessage(mockMsg);

            verify(mockReceiverClient).defer(mockMsg);
        }

        @Test
        @DisplayName("Defer failure wraps exception")
        void testDeferMessage_failure() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-fail");
            doThrow(new RuntimeException("defer failed")).when(mockReceiverClient).defer(mockMsg);

            assertThrows(RuntimeException.class, () -> client.deferMessage(mockMsg));
        }
    }

    @Nested
    @DisplayName("ReceiveDeferredMessage")
    class ReceiveDeferredMessageTests {

        @Test
        @DisplayName("Receives a deferred message by sequence number")
        void testReceiveDeferred_success() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("deferred-1");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("deferred body"));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);
            Map<String, Object> props = new HashMap<>();
            when(mockMsg.getApplicationProperties()).thenReturn(props);
            when(mockReceiverClient.receiveDeferredMessage(123L)).thenReturn(mockMsg);

            LargeServiceBusMessage result = client.receiveDeferredMessage(123L);

            assertNotNull(result);
            assertEquals("deferred body", result.getBody());
        }

        @Test
        @DisplayName("Returns null when deferred message not found")
        void testReceiveDeferred_notFound() {
            when(mockReceiverClient.receiveDeferredMessage(999L)).thenReturn(null);

            LargeServiceBusMessage result = client.receiveDeferredMessage(999L);

            assertNull(result);
        }

        @Test
        @DisplayName("Resolves blob-backed deferred message")
        void testReceiveDeferred_blobBacked() {
            BlobPointer pointer = new BlobPointer("c", "deferred-blob");
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("deferred-blob");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
            when(mockMsg.getDeliveryCount()).thenReturn(0L);

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
            appProps.put(config.getReservedAttributeName(), 500);
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn("resolved deferred");
            when(mockReceiverClient.receiveDeferredMessage(50L)).thenReturn(mockMsg);

            LargeServiceBusMessage result = client.receiveDeferredMessage(50L);

            assertNotNull(result);
            assertEquals("resolved deferred", result.getBody());
            assertTrue(result.isPayloadFromBlob());
        }

        @Test
        @DisplayName("Receive deferred failure wraps exception")
        void testReceiveDeferred_failure() {
            when(mockReceiverClient.receiveDeferredMessage(anyLong()))
                    .thenThrow(new RuntimeException("deferred failed"));

            assertThrows(RuntimeException.class, () -> client.receiveDeferredMessage(1L));
        }
    }

    @Nested
    @DisplayName("ReceiveDeferredMessages (batch)")
    class ReceiveDeferredMessagesTests {

        @Test
        @DisplayName("Receives multiple deferred messages")
        void testReceiveDeferred_multiple() {
            ServiceBusReceivedMessage msg1 = mock(ServiceBusReceivedMessage.class);
            when(msg1.getMessageId()).thenReturn("d1");
            when(msg1.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body1"));
            when(msg1.getDeliveryCount()).thenReturn(0L);
            when(msg1.getApplicationProperties()).thenReturn(new HashMap<>());

            ServiceBusReceivedMessage msg2 = mock(ServiceBusReceivedMessage.class);
            when(msg2.getMessageId()).thenReturn("d2");
            when(msg2.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body2"));
            when(msg2.getDeliveryCount()).thenReturn(0L);
            when(msg2.getApplicationProperties()).thenReturn(new HashMap<>());

            when(mockReceiverClient.receiveDeferredMessage(1L)).thenReturn(msg1);
            when(mockReceiverClient.receiveDeferredMessage(2L)).thenReturn(msg2);

            List<LargeServiceBusMessage> results = client.receiveDeferredMessages(List.of(1L, 2L));

            assertEquals(2, results.size());
        }

        @Test
        @DisplayName("Empty sequence number list returns empty list")
        void testReceiveDeferred_emptyList() {
            List<LargeServiceBusMessage> results = client.receiveDeferredMessages(new ArrayList<>());
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Null sequence number list returns empty list")
        void testReceiveDeferred_nullList() {
            List<LargeServiceBusMessage> results = client.receiveDeferredMessages(null);
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Handles partial failures in batch deferred receive")
        void testReceiveDeferred_partialFailure() {
            ServiceBusReceivedMessage msg1 = mock(ServiceBusReceivedMessage.class);
            when(msg1.getMessageId()).thenReturn("d1");
            when(msg1.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body1"));
            when(msg1.getDeliveryCount()).thenReturn(0L);
            when(msg1.getApplicationProperties()).thenReturn(new HashMap<>());

            when(mockReceiverClient.receiveDeferredMessage(1L)).thenReturn(msg1);
            when(mockReceiverClient.receiveDeferredMessage(2L))
                    .thenThrow(new RuntimeException("not found"));

            List<LargeServiceBusMessage> results = client.receiveDeferredMessages(List.of(1L, 2L));

            assertEquals(1, results.size());
        }

        @Test
        @DisplayName("Skips null deferred messages")
        void testReceiveDeferred_nullMessage() {
            when(mockReceiverClient.receiveDeferredMessage(1L)).thenReturn(null);

            List<LargeServiceBusMessage> results = client.receiveDeferredMessages(List.of(1L));

            assertTrue(results.isEmpty());
        }
    }

    @Nested
    @DisplayName("DeletePayloadBatch")
    class DeletePayloadBatchTests {

        @Test
        @DisplayName("Batch delete all successful")
        void testBatchDelete_allSuccess() {
            BlobPointer p1 = new BlobPointer("c", "b1");
            BlobPointer p2 = new BlobPointer("c", "b2");
            LargeServiceBusMessage m1 = new LargeServiceBusMessage("1", "a", Map.of(), true, p1);
            LargeServiceBusMessage m2 = new LargeServiceBusMessage("2", "b", Map.of(), true, p2);

            int deleted = client.deletePayloadBatch(List.of(m1, m2));

            assertEquals(2, deleted);
            verify(mockPayloadStore).deletePayload(p1);
            verify(mockPayloadStore).deletePayload(p2);
        }

        @Test
        @DisplayName("Batch delete with partial failure")
        void testBatchDelete_partialFailure() {
            BlobPointer p1 = new BlobPointer("c", "b1");
            BlobPointer p2 = new BlobPointer("c", "b2");
            LargeServiceBusMessage m1 = new LargeServiceBusMessage("1", "a", Map.of(), true, p1);
            LargeServiceBusMessage m2 = new LargeServiceBusMessage("2", "b", Map.of(), true, p2);

            doNothing().when(mockPayloadStore).deletePayload(p1);
            doThrow(new RuntimeException("storage error")).when(mockPayloadStore).deletePayload(p2);

            int deleted = client.deletePayloadBatch(List.of(m1, m2));

            assertEquals(1, deleted);
        }

        @Test
        @DisplayName("Batch delete empty list returns 0")
        void testBatchDelete_emptyList() {
            assertEquals(0, client.deletePayloadBatch(new ArrayList<>()));
        }

        @Test
        @DisplayName("Batch delete null list returns 0")
        void testBatchDelete_nullList() {
            assertEquals(0, client.deletePayloadBatch(null));
        }

        @Test
        @DisplayName("Batch delete skips when cleanup disabled")
        void testBatchDelete_cleanupDisabled() {
            config.setCleanupBlobOnDelete(false);
            BlobPointer p = new BlobPointer("c", "b");
            LargeServiceBusMessage msg = new LargeServiceBusMessage("1", "a", Map.of(), true, p);

            assertEquals(0, client.deletePayloadBatch(List.of(msg)));
            verify(mockPayloadStore, never()).deletePayload(any());
        }

        @Test
        @DisplayName("Batch delete skips when payload support disabled")
        void testBatchDelete_payloadSupportDisabled() {
            config.setPayloadSupportEnabled(false);
            BlobPointer p = new BlobPointer("c", "b");
            LargeServiceBusMessage msg = new LargeServiceBusMessage("1", "a", Map.of(), true, p);

            assertEquals(0, client.deletePayloadBatch(List.of(msg)));
            verify(mockPayloadStore, never()).deletePayload(any());
        }

        @Test
        @DisplayName("Batch delete skips non-blob messages")
        void testBatchDelete_mixedBlobAndNonBlob() {
            BlobPointer p = new BlobPointer("c", "b");
            LargeServiceBusMessage blobMsg = new LargeServiceBusMessage("1", "a", Map.of(), true, p);
            LargeServiceBusMessage directMsg = new LargeServiceBusMessage("2", "b", Map.of(), false, null);

            int deleted = client.deletePayloadBatch(List.of(blobMsg, directMsg));

            assertEquals(1, deleted);
            verify(mockPayloadStore, times(1)).deletePayload(any());
        }
    }

    @Nested
    @DisplayName("RenewMessageLock")
    class RenewMessageLockTests {

        @Test
        @DisplayName("Renews lock and returns new expiration")
        void testRenewLock_success() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-lock");
            OffsetDateTime newExpiry = OffsetDateTime.now().plusMinutes(5);
            when(mockReceiverClient.renewMessageLock(mockMsg)).thenReturn(newExpiry);

            OffsetDateTime result = client.renewMessageLock(mockMsg);

            assertEquals(newExpiry, result);
        }

        @Test
        @DisplayName("Renew lock failure wraps exception")
        void testRenewLock_failure() {
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("msg-fail");
            when(mockReceiverClient.renewMessageLock(mockMsg))
                    .thenThrow(new RuntimeException("lock renewal failed"));

            assertThrows(RuntimeException.class, () -> client.renewMessageLock(mockMsg));
        }
    }

    @Nested
    @DisplayName("RenewMessageLockBatch")
    class RenewMessageLockBatchTests {

        @Test
        @DisplayName("Renews locks for multiple messages")
        void testRenewLockBatch_allSuccess() {
            ServiceBusReceivedMessage msg1 = mock(ServiceBusReceivedMessage.class);
            when(msg1.getMessageId()).thenReturn("m1");
            ServiceBusReceivedMessage msg2 = mock(ServiceBusReceivedMessage.class);
            when(msg2.getMessageId()).thenReturn("m2");

            OffsetDateTime t1 = OffsetDateTime.now().plusMinutes(5);
            OffsetDateTime t2 = OffsetDateTime.now().plusMinutes(6);
            when(mockReceiverClient.renewMessageLock(msg1)).thenReturn(t1);
            when(mockReceiverClient.renewMessageLock(msg2)).thenReturn(t2);

            Map<String, OffsetDateTime> results = client.renewMessageLockBatch(List.of(msg1, msg2));

            assertEquals(2, results.size());
            assertEquals(t1, results.get("m1"));
            assertEquals(t2, results.get("m2"));
        }

        @Test
        @DisplayName("Handles partial lock renewal failure")
        void testRenewLockBatch_partialFailure() {
            ServiceBusReceivedMessage msg1 = mock(ServiceBusReceivedMessage.class);
            when(msg1.getMessageId()).thenReturn("m1");
            ServiceBusReceivedMessage msg2 = mock(ServiceBusReceivedMessage.class);
            when(msg2.getMessageId()).thenReturn("m2");

            OffsetDateTime t1 = OffsetDateTime.now().plusMinutes(5);
            when(mockReceiverClient.renewMessageLock(msg1)).thenReturn(t1);
            when(mockReceiverClient.renewMessageLock(msg2))
                    .thenThrow(new RuntimeException("renewal failed"));

            Map<String, OffsetDateTime> results = client.renewMessageLockBatch(List.of(msg1, msg2));

            assertEquals(1, results.size());
            assertTrue(results.containsKey("m1"));
        }

        @Test
        @DisplayName("Empty list returns empty map")
        void testRenewLockBatch_emptyList() {
            Map<String, OffsetDateTime> results = client.renewMessageLockBatch(new ArrayList<>());
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Null list returns empty map")
        void testRenewLockBatch_nullList() {
            Map<String, OffsetDateTime> results = client.renewMessageLockBatch(null);
            assertTrue(results.isEmpty());
        }
    }

    @Nested
    @DisplayName("ProcessMessages")
    class ProcessMessagesTests {

        @Test
        @DisplayName("3-param processMessages throws UnsupportedOperationException")
        void testProcessMessages_3param_throwsUnsupported() {
            assertThrows(UnsupportedOperationException.class,
                    () -> client.processMessages("conn", msg -> {}, err -> {}));
        }

        @Test
        @DisplayName("4-param processMessages creates and starts a processor")
        void testProcessMessages_4param_startsProcessor() {
            ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusProcessorClientBuilder procBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder.class);
                        when(builder.processor()).thenReturn(procBuilder);
                        when(procBuilder.queueName(anyString())).thenReturn(procBuilder);
                        when(procBuilder.processMessage(any())).thenReturn(procBuilder);
                        when(procBuilder.processError(any())).thenReturn(procBuilder);
                        when(procBuilder.buildProcessorClient()).thenReturn(mockProcessor);
                    })) {

                client.processMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", msg -> {}, err -> {});

                verify(mockProcessor).start();
            }
        }

        @Test
        @DisplayName("4-param processMessages closes existing processor before creating new one")
        void testProcessMessages_4param_closesExistingProcessor() {
            ServiceBusProcessorClient mockProcessor1 = mock(ServiceBusProcessorClient.class);
            ServiceBusProcessorClient mockProcessor2 = mock(ServiceBusProcessorClient.class);

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusProcessorClientBuilder procBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder.class);
                        when(builder.processor()).thenReturn(procBuilder);
                        when(procBuilder.queueName(anyString())).thenReturn(procBuilder);
                        when(procBuilder.processMessage(any())).thenReturn(procBuilder);
                        when(procBuilder.processError(any())).thenReturn(procBuilder);
                        // First construction returns mockProcessor1, second returns mockProcessor2
                        when(procBuilder.buildProcessorClient())
                                .thenReturn(ctx.getCount() == 1 ? mockProcessor1 : mockProcessor2);
                    })) {

                String connStr = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==";

                // First call — starts processor 1
                client.processMessages(connStr, "queue1", msg -> {}, err -> {});
                verify(mockProcessor1).start();

                // Second call — should close processor 1 and start processor 2
                client.processMessages(connStr, "queue2", msg -> {}, err -> {});
                verify(mockProcessor1).close();
            }
        }
    }

    @Nested
    @DisplayName("ReceiveDeadLetterMessages")
    class ReceiveDeadLetterMessagesTests {

        @Test
        @DisplayName("Creates DLQ receiver and returns resolved messages")
        void testReceiveDeadLetterMessages_success() {
            ServiceBusReceiverClient mockDlqReceiver = mock(ServiceBusReceiverClient.class);
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("dlq-1");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("dead body"));
            when(mockMsg.getDeliveryCount()).thenReturn(3L);
            when(mockMsg.getDeadLetterReason()).thenReturn("MaxDeliveryCountExceeded");
            when(mockMsg.getDeadLetterErrorDescription()).thenReturn("Exceeded max delivery");
            Map<String, Object> appProps = new HashMap<>();
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockDlqReceiver.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusReceiverClientBuilder recvBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusReceiverClientBuilder.class);
                        when(builder.receiver()).thenReturn(recvBuilder);
                        when(recvBuilder.queueName(anyString())).thenReturn(recvBuilder);
                        when(recvBuilder.subQueue(any())).thenReturn(recvBuilder);
                        when(recvBuilder.buildClient()).thenReturn(mockDlqReceiver);
                    })) {

                List<LargeServiceBusMessage> results = client.receiveDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", 10);

                assertEquals(1, results.size());
                assertEquals("dead body", results.get(0).getBody());
                assertEquals("MaxDeliveryCountExceeded", results.get(0).getDeadLetterReason());
                assertEquals(3, results.get(0).getDeliveryCount());
            }
        }

        @Test
        @DisplayName("Returns empty list when no DLQ messages available")
        void testReceiveDeadLetterMessages_empty() {
            ServiceBusReceiverClient mockDlqReceiver = mock(ServiceBusReceiverClient.class);
            when(mockDlqReceiver.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of()));

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusReceiverClientBuilder recvBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusReceiverClientBuilder.class);
                        when(builder.receiver()).thenReturn(recvBuilder);
                        when(recvBuilder.queueName(anyString())).thenReturn(recvBuilder);
                        when(recvBuilder.subQueue(any())).thenReturn(recvBuilder);
                        when(recvBuilder.buildClient()).thenReturn(mockDlqReceiver);
                    })) {

                List<LargeServiceBusMessage> results = client.receiveDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", 5);

                assertTrue(results.isEmpty());
            }
        }

        @Test
        @DisplayName("Wraps exception from DLQ receiver in RuntimeException")
        void testReceiveDeadLetterMessages_failure() {
            ServiceBusReceiverClient mockDlqReceiver = mock(ServiceBusReceiverClient.class);
            when(mockDlqReceiver.receiveMessages(anyInt(), any(Duration.class)))
                    .thenThrow(new RuntimeException("DLQ receive error"));

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusReceiverClientBuilder recvBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusReceiverClientBuilder.class);
                        when(builder.receiver()).thenReturn(recvBuilder);
                        when(recvBuilder.queueName(anyString())).thenReturn(recvBuilder);
                        when(recvBuilder.subQueue(any())).thenReturn(recvBuilder);
                        when(recvBuilder.buildClient()).thenReturn(mockDlqReceiver);
                    })) {

                assertThrows(RuntimeException.class, () -> client.receiveDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", 10));
            }
        }

        @Test
        @DisplayName("Resolves blob-backed messages from DLQ")
        void testReceiveDeadLetterMessages_blobBacked() {
            BlobPointer pointer = new BlobPointer("c", "dlq-blob");
            ServiceBusReceiverClient mockDlqReceiver = mock(ServiceBusReceiverClient.class);
            ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
            when(mockMsg.getMessageId()).thenReturn("dlq-blob-1");
            when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
            when(mockMsg.getDeliveryCount()).thenReturn(5L);
            when(mockMsg.getDeadLetterReason()).thenReturn("ProcessingFailure");
            when(mockMsg.getDeadLetterErrorDescription()).thenReturn("Error");

            Map<String, Object> appProps = new HashMap<>();
            appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
            appProps.put(config.getReservedAttributeName(), 5000);
            when(mockMsg.getApplicationProperties()).thenReturn(appProps);
            when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn("resolved DLQ body");
            when(mockDlqReceiver.receiveMessages(anyInt(), any(Duration.class)))
                    .thenReturn(new IterableStream<>(List.of(mockMsg)));

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusReceiverClientBuilder recvBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusReceiverClientBuilder.class);
                        when(builder.receiver()).thenReturn(recvBuilder);
                        when(recvBuilder.queueName(anyString())).thenReturn(recvBuilder);
                        when(recvBuilder.subQueue(any())).thenReturn(recvBuilder);
                        when(recvBuilder.buildClient()).thenReturn(mockDlqReceiver);
                    })) {

                List<LargeServiceBusMessage> results = client.receiveDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", 10);

                assertEquals(1, results.size());
                assertEquals("resolved DLQ body", results.get(0).getBody());
                assertTrue(results.get(0).isPayloadFromBlob());
                assertEquals("ProcessingFailure", results.get(0).getDeadLetterReason());
            }
        }
    }

    @Nested
    @DisplayName("ProcessDeadLetterMessages")
    class ProcessDeadLetterMessagesTests {

        @Test
        @DisplayName("Creates DLQ processor and starts it")
        void testProcessDeadLetterMessages_startsProcessor() {
            ServiceBusProcessorClient mockDlqProcessor = mock(ServiceBusProcessorClient.class);

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusProcessorClientBuilder procBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder.class);
                        when(builder.processor()).thenReturn(procBuilder);
                        when(procBuilder.queueName(anyString())).thenReturn(procBuilder);
                        when(procBuilder.subQueue(any())).thenReturn(procBuilder);
                        when(procBuilder.processMessage(any())).thenReturn(procBuilder);
                        when(procBuilder.processError(any())).thenReturn(procBuilder);
                        when(procBuilder.buildProcessorClient()).thenReturn(mockDlqProcessor);
                    })) {

                ServiceBusProcessorClient result = client.processDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", msg -> {}, err -> {});

                assertNotNull(result);
                verify(mockDlqProcessor).start();
            }
        }

        @Test
        @DisplayName("Returns the started DLQ processor for caller to manage")
        void testProcessDeadLetterMessages_returnsProcessor() {
            ServiceBusProcessorClient mockDlqProcessor = mock(ServiceBusProcessorClient.class);

            try (MockedConstruction<ServiceBusClientBuilder> mocked = mockConstruction(
                    ServiceBusClientBuilder.class, (builder, ctx) -> {
                        when(builder.connectionString(anyString())).thenReturn(builder);
                        ServiceBusClientBuilder.ServiceBusProcessorClientBuilder procBuilder =
                                mock(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder.class);
                        when(builder.processor()).thenReturn(procBuilder);
                        when(procBuilder.queueName(anyString())).thenReturn(procBuilder);
                        when(procBuilder.subQueue(any())).thenReturn(procBuilder);
                        when(procBuilder.processMessage(any())).thenReturn(procBuilder);
                        when(procBuilder.processError(any())).thenReturn(procBuilder);
                        when(procBuilder.buildProcessorClient()).thenReturn(mockDlqProcessor);
                    })) {

                ServiceBusProcessorClient result = client.processDeadLetterMessages(
                        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdA==",
                        "test-queue", msg -> {}, err -> {});

                assertSame(mockDlqProcessor, result);
            }
        }
    }

    @Nested
    @DisplayName("Close")
    class CloseTests {

        @Test
        @DisplayName("Closes sender and receiver clients")
        void testClose_releasesClients() {
            client.close();

            verify(mockSenderClient).close();
            verify(mockReceiverClient).close();
        }

        @Test
        @DisplayName("Close handles exceptions gracefully")
        void testClose_handlesException() {
            doThrow(new RuntimeException("close failed")).when(mockSenderClient).close();

            // Should not throw
            assertDoesNotThrow(() -> client.close());
        }
    }
}
