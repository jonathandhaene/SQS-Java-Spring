/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.integration;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Local end-to-end integration tests that exercise the full
 * {@link AzureServiceBusLargeMessageClient} pipeline with mocked
 * external dependencies (Service Bus sender/receiver &amp; Blob Storage).
 *
 * <p>These tests run without any Azure credentials and can be executed
 * locally as part of a regular build via:
 * <pre>mvn verify -Pintegration-test-local</pre>
 */
@DisplayName("Local End-to-End Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LocalEndToEndIntegrationTest extends IntegrationTestBase {

    private ServiceBusSenderClient mockSender;
    private ServiceBusReceiverClient mockReceiver;
    private BlobPayloadStore mockPayloadStore;
    private LargeMessageClientConfiguration config;
    private AzureServiceBusLargeMessageClient client;

    @BeforeEach
    void setUp() {
        mockSender = mock(ServiceBusSenderClient.class);
        mockReceiver = mock(ServiceBusReceiverClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
        config = createTestConfiguration();
        client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    // =========================================================================
    // Send path
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Small message is sent directly without blob offloading")
    void testSmallMessageDirectSend() {
        String body = generateSmallMessage();
        client.sendMessage(body);

        // Blob store should not be touched
        verifyNoInteractions(mockPayloadStore);

        // Message should be sent
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());

        ServiceBusMessage sent = captor.getValue();
        assertEquals(body, sent.getBody().toString());
        assertNull(sent.getApplicationProperties()
                .get(LargeMessageClientConfiguration.BLOB_POINTER_MARKER));

        logger.info("✓ Small message sent directly ({} bytes)", body.length());
    }

    @Test
    @Order(2)
    @DisplayName("Large message is offloaded to blob storage")
    void testLargeMessageBlobOffload() {
        String body = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);

        client.sendMessage(body);

        // Blob store should receive the payload
        verify(mockPayloadStore).storePayload(anyString(), eq(body));

        // Sent message should contain the blob pointer, not the original body
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());

        ServiceBusMessage sent = captor.getValue();
        assertNotEquals(body, sent.getBody().toString());
        assertEquals("true", sent.getApplicationProperties()
                .get(LargeMessageClientConfiguration.BLOB_POINTER_MARKER));

        logger.info("✓ Large message offloaded to blob storage ({} bytes)", body.length());
    }

    @Test
    @Order(3)
    @DisplayName("Message with application properties preserves them")
    void testSendWithApplicationProperties() {
        String body = generateSmallMessage();
        Map<String, Object> props = new HashMap<>();
        props.put("orderId", "ORD-123");
        props.put("priority", 5);

        client.sendMessage(body, props);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());

        Map<String, Object> sentProps = captor.getValue().getApplicationProperties();
        assertEquals("ORD-123", sentProps.get("orderId"));
        assertEquals(5, sentProps.get("priority"));

        logger.info("✓ Application properties preserved on sent message");
    }

    @Test
    @Order(4)
    @DisplayName("Message with session ID is forwarded correctly")
    void testSendWithSessionId() {
        String body = generateSmallMessage();
        String sessionId = "session-42";

        client.sendMessage(body, sessionId);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertEquals(sessionId, captor.getValue().getSessionId());

        logger.info("✓ Session ID set on sent message");
    }

    @Test
    @Order(5)
    @DisplayName("Always-through-blob mode forces offloading even for small messages")
    void testAlwaysThroughBlobMode() {
        config.setAlwaysThroughBlob(true);
        String body = generateSmallMessage();
        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);

        client.sendMessage(body);

        verify(mockPayloadStore).storePayload(anyString(), eq(body));

        logger.info("✓ Small message offloaded when alwaysThroughBlob=true");
    }

    @Test
    @Order(6)
    @DisplayName("Duplicate detection sets content-hash message ID")
    void testDuplicateDetectionId() {
        config.setEnableDuplicateDetectionId(true);
        String body = generateSmallMessage();

        client.sendMessage(body);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertNotNull(captor.getValue().getMessageId(), "Message ID should be set");

        logger.info("✓ Duplicate detection hash set as message ID");
    }

    @Test
    @Order(7)
    @DisplayName("Reserved property name in user properties is rejected")
    void testReservedPropertyRejection() {
        Map<String, Object> props = new HashMap<>();
        props.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "bad");

        assertThrows(RuntimeException.class, () ->
                client.sendMessage(generateSmallMessage(), props));

        logger.info("✓ Reserved property name correctly rejected");
    }

    @Test
    @Order(8)
    @DisplayName("Scheduled message returns sequence number")
    void testScheduledMessage() {
        String body = generateSmallMessage();
        OffsetDateTime time = OffsetDateTime.now().plusMinutes(5);
        when(mockSender.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                .thenReturn(42L);

        Long seq = client.sendScheduledMessage(body, time);

        assertEquals(42L, seq);
        verify(mockSender).scheduleMessage(any(ServiceBusMessage.class), eq(time));

        logger.info("✓ Scheduled message returned sequence number {}", seq);
    }

    // =========================================================================
    // Batch send path
    // =========================================================================

    @Test
    @Order(9)
    @DisplayName("Batch send with mixed large/small messages")
    void testBatchSendMixedSizes() {
        String small = generateSmallMessage();
        String large = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), eq(large))).thenReturn(pointer);

        // Mock batch creation
        ServiceBusMessageBatch batch = mock(ServiceBusMessageBatch.class);
        when(batch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
        when(mockSender.createMessageBatch()).thenReturn(batch);

        client.sendMessageBatch(List.of(small, large));

        // Large message should be offloaded
        verify(mockPayloadStore).storePayload(anyString(), eq(large));
        // Batch should be sent
        verify(mockSender).sendMessages(any(ServiceBusMessageBatch.class));

        logger.info("✓ Batch with mixed sizes processed correctly");
    }

    // =========================================================================
    // Receive path
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Receive resolves blob-backed message payload")
    void testReceiveResolveBlobMessage() {
        String originalPayload = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "blob-123");

        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("msg-1");
        when(mockMsg.getBody())
                .thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
        when(mockMsg.getDeliveryCount()).thenReturn(1L);

        Map<String, Object> appProps = new HashMap<>();
        appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
        appProps.put(config.getReservedAttributeName(),
                originalPayload.getBytes(StandardCharsets.UTF_8).length);
        appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                LargeMessageClientConfiguration.USER_AGENT_VALUE);
        when(mockMsg.getApplicationProperties()).thenReturn(appProps);
        when(mockPayloadStore.getPayload(any(BlobPointer.class))).thenReturn(originalPayload);

        when(mockReceiver.receiveMessages(anyInt(), any(Duration.class)))
                .thenReturn(new IterableStream<>(List.of(mockMsg)));

        List<LargeServiceBusMessage> messages = client.receiveMessages(10);

        assertEquals(1, messages.size());
        LargeServiceBusMessage received = messages.getFirst();
        assertEquals(originalPayload, received.getBody());
        assertTrue(received.isPayloadFromBlob());
        assertNotNull(received.getBlobPointer());

        logger.info("✓ Blob-backed message resolved (body {} bytes)", received.getBody().length());
    }

    @Test
    @Order(11)
    @DisplayName("Receive non-blob message passes body through directly")
    void testReceiveDirectMessage() {
        String body = generateSmallMessage();

        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("msg-direct");
        when(mockMsg.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString(body));
        when(mockMsg.getDeliveryCount()).thenReturn(1L);

        Map<String, Object> props = new HashMap<>();
        props.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                LargeMessageClientConfiguration.USER_AGENT_VALUE);
        when(mockMsg.getApplicationProperties()).thenReturn(props);

        when(mockReceiver.receiveMessages(anyInt(), any(Duration.class)))
                .thenReturn(new IterableStream<>(List.of(mockMsg)));

        List<LargeServiceBusMessage> messages = client.receiveMessages(10);

        assertEquals(1, messages.size());
        assertEquals(body, messages.getFirst().getBody());
        assertFalse(messages.getFirst().isPayloadFromBlob());

        logger.info("✓ Non-blob message received directly");
    }

    // =========================================================================
    // Delete / Cleanup path
    // =========================================================================

    @Test
    @Order(12)
    @DisplayName("Delete payload removes the blob for an offloaded message")
    void testDeletePayload() {
        BlobPointer pointer = new BlobPointer("c", "blob-del");
        LargeServiceBusMessage msg = new LargeServiceBusMessage(
                "msg-x", "body", Map.of(), true, pointer);

        client.deletePayload(msg);

        verify(mockPayloadStore).deletePayload(pointer);

        logger.info("✓ Blob deleted for offloaded message");
    }

    @Test
    @Order(13)
    @DisplayName("Delete payload is skipped for non-blob message")
    void testDeletePayloadSkipsNonBlob() {
        LargeServiceBusMessage msg = new LargeServiceBusMessage(
                "msg-y", "body", Map.of(), false, null);

        client.deletePayload(msg);

        verifyNoInteractions(mockPayloadStore);

        logger.info("✓ Delete skipped for non-blob message");
    }

    @Test
    @Order(14)
    @DisplayName("Batch delete reports partial failures")
    void testBatchDeletePartialFailure() {
        BlobPointer p1 = new BlobPointer("c", "b1");
        BlobPointer p2 = new BlobPointer("c", "b2");
        LargeServiceBusMessage m1 = new LargeServiceBusMessage("1", "a", Map.of(), true, p1);
        LargeServiceBusMessage m2 = new LargeServiceBusMessage("2", "b", Map.of(), true, p2);

        doNothing().when(mockPayloadStore).deletePayload(p1);
        doThrow(new RuntimeException("transient")).when(mockPayloadStore).deletePayload(p2);

        int deleted = client.deletePayloadBatch(List.of(m1, m2));

        assertEquals(1, deleted, "Only one blob should have been deleted successfully");

        logger.info("✓ Batch delete handled partial failure (1 ok, 1 failed)");
    }

    // =========================================================================
    // Retry behaviour
    // =========================================================================

    @Test
    @Order(15)
    @DisplayName("Transient failure is retried and succeeds")
    void testRetryOnTransientFailure() {
        String body = generateSmallMessage();

        doThrow(new RuntimeException("transient"))
                .doNothing()
                .when(mockSender).sendMessage(any(ServiceBusMessage.class));

        assertDoesNotThrow(() -> client.sendMessage(body));

        verify(mockSender, times(2)).sendMessage(any(ServiceBusMessage.class));

        logger.info("✓ Transient failure retried and succeeded on 2nd attempt");
    }

    // =========================================================================
    // Close / lifecycle
    // =========================================================================

    @Test
    @Order(16)
    @DisplayName("Close releases underlying Service Bus clients")
    void testCloseReleasesClients() {
        client.close();

        verify(mockSender).close();
        verify(mockReceiver).close();

        // Prevent double-close in @AfterEach
        client = null;

        logger.info("✓ Client resources released on close()");
    }
}
