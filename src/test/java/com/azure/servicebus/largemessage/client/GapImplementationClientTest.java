/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.client;

import com.azure.messaging.servicebus.ServiceBusMessage;
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

import com.azure.messaging.servicebus.ServiceBusMessageBatch;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for gap implementations in AzureServiceBusLargeMessageClient:
 * - Transactional atomicity (orphan blob cleanup on send failure)
 * - Binary (byte[]) payload support
 * - Auto-cleanup on message completion
 * - SAS URI generation in batch send
 */
class GapImplementationClientTest {

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
        config.setMessageSizeThreshold(1024); // 1 KB
        config.setAlwaysThroughBlob(false);
        config.setCleanupBlobOnDelete(true);
        config.setBlobKeyPrefix("");
        config.setUseLegacyReservedAttributeName(false);

        client = new AzureServiceBusLargeMessageClient(
                mockSenderClient,
                mockReceiverClient,
                mockPayloadStore,
                config
        );
    }

    // ========== Gap 1: Transactional Atomicity Tests ==========

    @Nested
    @DisplayName("Gap 1: Transactional Atomicity — Orphan Blob Cleanup")
    class TransactionalAtomicityTests {

        @Test
        @DisplayName("Should clean up orphaned blob when Service Bus send fails after blob upload")
        void testSendFailure_cleansUpOrphanedBlob() {
            // Arrange
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "orphan-blob");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            doThrow(new RuntimeException("Service Bus unavailable"))
                    .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

            // Act & Assert
            assertThrows(RuntimeException.class, () -> client.sendMessage(largeMessage));

            // Verify the orphaned blob was cleaned up
            verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        }

        @Test
        @DisplayName("Should not attempt blob cleanup for small messages that are not offloaded")
        void testSmallMessageFailure_noOrphanCleanup() {
            // Arrange
            String smallMessage = "Small message";
            doThrow(new RuntimeException("Send failed"))
                    .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

            // Act & Assert
            assertThrows(RuntimeException.class, () -> client.sendMessage(smallMessage));

            // Verify no blob interactions
            verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
            verify(mockPayloadStore, never()).deletePayload(any());
        }

        @Test
        @DisplayName("Should log warning but still throw when orphan cleanup itself fails")
        void testSendFailure_orphanCleanupAlsoFails() {
            // Arrange
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "orphan-blob");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            doThrow(new RuntimeException("Service Bus down"))
                    .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));
            doThrow(new RuntimeException("Storage also down"))
                    .when(mockPayloadStore).deletePayload(pointer);

            // Act & Assert — an exception should propagate (may be wrapped by retry handler)
            assertThrows(RuntimeException.class,
                    () -> client.sendMessage(largeMessage));

            // Verify cleanup was attempted
            verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        }

        @Test
        @DisplayName("Should not clean up blob when send succeeds")
        void testSuccessfulSend_noOrphanCleanup() {
            // Arrange
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "good-blob");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);

            // Act
            client.sendMessage(largeMessage);

            // Assert
            verify(mockPayloadStore, never()).deletePayload(any());
        }
    }

    // ========== Gap 2: Binary (byte[]) Payload Support Tests ==========

    @Nested
    @DisplayName("Gap 2: Binary Payload Support")
    class BinaryPayloadTests {

        @Test
        @DisplayName("Should send small binary message directly without offloading")
        void testSmallBinaryMessage_sentDirectly() {
            // Arrange
            byte[] smallPayload = "Small binary".getBytes(StandardCharsets.UTF_8);

            // Act
            client.sendBinaryMessage(smallPayload, "application/octet-stream");

            // Assert
            verify(mockSenderClient, times(1)).sendMessage(any(ServiceBusMessage.class));
            verify(mockPayloadStore, never()).storeBinaryPayload(anyString(), any(byte[].class), anyString());
        }

        @Test
        @DisplayName("Should offload large binary message to blob storage")
        void testLargeBinaryMessage_offloadedToBlob() {
            // Arrange
            byte[] largePayload = new byte[2048];
            BlobPointer pointer = new BlobPointer("test-container", "binary-blob");

            when(mockPayloadStore.storeBinaryPayload(anyString(), eq(largePayload), eq("application/avro")))
                    .thenReturn(pointer);

            ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

            // Act
            client.sendBinaryMessage(largePayload, "application/avro");

            // Assert
            verify(mockPayloadStore, times(1)).storeBinaryPayload(anyString(), eq(largePayload), eq("application/avro"));
            verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());

            ServiceBusMessage capturedMessage = messageCaptor.getValue();
            assertEquals("true", capturedMessage.getApplicationProperties().get(
                    LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
            assertEquals("application/avro", capturedMessage.getApplicationProperties().get("contentType"));
        }

        @Test
        @DisplayName("Should include custom application properties in binary message")
        void testBinaryMessage_withApplicationProperties() {
            // Arrange
            byte[] payload = "Small binary".getBytes(StandardCharsets.UTF_8);
            Map<String, Object> props = new HashMap<>();
            props.put("tenantId", "tenant-123");

            ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

            // Act
            client.sendBinaryMessage(payload, "application/protobuf", props);

            // Assert
            verify(mockSenderClient).sendMessage(messageCaptor.capture());
            assertEquals("tenant-123", messageCaptor.getValue().getApplicationProperties().get("tenantId"));
            assertEquals("application/protobuf", messageCaptor.getValue().getApplicationProperties().get("contentType"));
        }

        @Test
        @DisplayName("Should clean up orphaned blob when binary send fails")
        void testBinarySendFailure_cleansUpOrphanedBlob() {
            // Arrange
            byte[] largePayload = new byte[2048];
            BlobPointer pointer = new BlobPointer("test-container", "orphan-binary-blob");

            when(mockPayloadStore.storeBinaryPayload(anyString(), eq(largePayload), anyString()))
                    .thenReturn(pointer);
            doThrow(new RuntimeException("Send failed"))
                    .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

            // Act & Assert
            assertThrows(RuntimeException.class,
                    () -> client.sendBinaryMessage(largePayload, "application/octet-stream"));

            verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        }
    }

    // ========== Gap 3: Auto Blob Cleanup Tests ==========

    @Nested
    @DisplayName("Gap 3: Auto Blob Cleanup on Complete")
    class AutoCleanupTests {

        @Test
        @DisplayName("Configuration should default autoCleanupOnComplete to false")
        void testAutoCleanupDefault() {
            assertFalse(config.isAutoCleanupOnComplete());
        }

        @Test
        @DisplayName("Configuration should allow enabling autoCleanupOnComplete")
        void testAutoCleanupEnabled() {
            config.setAutoCleanupOnComplete(true);
            assertTrue(config.isAutoCleanupOnComplete());
        }
    }

    // ========== Gap 6: Batch Send with SAS Tests ==========

    @Nested
    @DisplayName("Gap 6: Batch Send with SAS URI")
    class BatchSendSasTests {

        @Test
        @DisplayName("Should generate SAS URI for offloaded messages in batch send")
        void testBatchSend_generatesSasForOffloadedMessages() {
            // Arrange
            config.setSasEnabled(true);
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "batch-blob");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                    .thenReturn("https://storage.blob.core.windows.net/container/blob?sv=...");

            // Mock the ServiceBusMessageBatch
            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            // Re-create client with SAS-enabled config
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);

            // Act
            java.util.List<String> messages = java.util.List.of(largeMessage);
            client.sendMessageBatch(messages);

            // Assert
            verify(mockPayloadStore, times(1)).generateSasUri(eq(pointer), any());
        }

        @Test
        @DisplayName("Should not generate SAS URI when SAS is disabled in batch")
        void testBatchSend_noSasWhenDisabled() {
            config.setSasEnabled(false);
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "batch-blob-no-sas");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);

            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            client.sendMessageBatch(java.util.List.of(largeMessage));

            verify(mockPayloadStore, never()).generateSasUri(any(), any());
        }

        @Test
        @DisplayName("Should handle SAS URI generation failure in batch gracefully")
        void testBatchSend_sasGenerationFails_continuesWithout() {
            config.setSasEnabled(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("test-container", "batch-sas-fail");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            when(mockPayloadStore.generateSasUri(any(), any()))
                    .thenThrow(new RuntimeException("SAS generation failed"));

            ServiceBusMessageBatch mockBatch = mock(ServiceBusMessageBatch.class);
            when(mockBatch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
            when(mockSenderClient.createMessageBatch()).thenReturn(mockBatch);

            // Should NOT throw — SAS failures swallowed in batch
            assertDoesNotThrow(() -> client.sendMessageBatch(java.util.List.of(largeMessage)));
        }
    }

    // ========== Binary send with SAS URI ==========

    @Nested
    @DisplayName("Gap 2 + Gap 6: Binary Send with SAS")
    class BinarySendWithSasTests {

        @Test
        @DisplayName("Should generate SAS URI for offloaded binary messages")
        void testBinarySend_withSas() {
            config.setSasEnabled(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);
            byte[] largePayload = new byte[2048];
            BlobPointer pointer = new BlobPointer("c", "binary-sas");

            when(mockPayloadStore.storeBinaryPayload(anyString(), eq(largePayload), anyString()))
                    .thenReturn(pointer);
            when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                    .thenReturn("https://store.blob.core.windows.net/c/binary?sv=...");

            ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

            client.sendBinaryMessage(largePayload, "application/octet-stream");

            verify(mockSenderClient).sendMessage(captor.capture());
            assertNotNull(captor.getValue().getApplicationProperties()
                    .get(config.getMessagePropertyForBlobSasUri()));
        }

        @Test
        @DisplayName("Binary send with duplicate detection sets message ID")
        void testBinarySend_withDuplicateDetection() {
            config.setEnableDuplicateDetectionId(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);
            byte[] payload = "small binary".getBytes(StandardCharsets.UTF_8);

            ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

            client.sendBinaryMessage(payload, "application/octet-stream");

            verify(mockSenderClient).sendMessage(captor.capture());
            assertNotNull(captor.getValue().getMessageId());
        }
    }

    // ========== Large message with Session ID ==========

    @Nested
    @DisplayName("Gap 1 + Session ID: Atomicity with Session")
    class AtomicityWithSessionTests {

        @Test
        @DisplayName("Large message with session ID: orphan cleanup on send failure")
        void testLargeMessageWithSessionId_sendFails_cleansUpOrphanedBlob() {
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("c", "session-orphan");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            doThrow(new RuntimeException("send failed"))
                    .when(mockSenderClient).sendMessage(any(ServiceBusMessage.class));

            assertThrows(RuntimeException.class,
                    () -> client.sendMessage(largeMessage, "session-1", new HashMap<>()));

            verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        }

        @Test
        @DisplayName("Large message with session ID and SAS: successful send")
        void testLargeMessageWithSessionIdAndSas() {
            config.setSasEnabled(true);
            client = new AzureServiceBusLargeMessageClient(
                    mockSenderClient, mockReceiverClient, mockPayloadStore, config);
            String largeMessage = generateMessage(2048);
            BlobPointer pointer = new BlobPointer("c", "session-sas");

            when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                    .thenReturn(pointer);
            when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                    .thenReturn("https://store.blob.core.windows.net/c/b?sv=...");

            ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

            client.sendMessage(largeMessage, "session-sas-1", new HashMap<>());

            verify(mockSenderClient).sendMessage(captor.capture());
            assertEquals("session-sas-1", captor.getValue().getSessionId());
            assertNotNull(captor.getValue().getApplicationProperties()
                    .get(config.getMessagePropertyForBlobSasUri()));
        }
    }

    // ========== Helper methods ==========

    private String generateMessage(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
