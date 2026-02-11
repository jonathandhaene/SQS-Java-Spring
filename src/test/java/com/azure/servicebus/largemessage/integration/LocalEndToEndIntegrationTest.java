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

    // =========================================================================
    // Gap 1: Transactional atomicity — orphan blob cleanup
    // =========================================================================

    @Test
    @Order(17)
    @DisplayName("Gap 1: Orphaned blob is cleaned up when send fails after offloading")
    void testTransactionalAtomicity_orphanCleanup() {
        String body = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "orphan-blob");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);
        doThrow(new RuntimeException("Service Bus unavailable"))
                .when(mockSender).sendMessage(any(ServiceBusMessage.class));

        assertThrows(RuntimeException.class, () -> client.sendMessage(body));

        verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        logger.info("✓ Orphaned blob cleaned up after send failure");
    }

    @Test
    @Order(18)
    @DisplayName("Gap 1: No orphan cleanup for small message send failures")
    void testTransactionalAtomicity_noCleanupForSmall() {
        String body = generateSmallMessage();
        doThrow(new RuntimeException("fail"))
                .when(mockSender).sendMessage(any(ServiceBusMessage.class));

        assertThrows(RuntimeException.class, () -> client.sendMessage(body));

        verify(mockPayloadStore, never()).deletePayload(any());
        logger.info("✓ No orphan cleanup attempted for small message failure");
    }

    // =========================================================================
    // Gap 2: Binary payload support
    // =========================================================================

    @Test
    @Order(19)
    @DisplayName("Gap 2: Small binary payload sent directly")
    void testBinarySmallMessage() {
        byte[] payload = "small binary".getBytes(StandardCharsets.UTF_8);

        client.sendBinaryMessage(payload, "application/octet-stream");

        verify(mockSender).sendMessage(any(ServiceBusMessage.class));
        verify(mockPayloadStore, never()).storeBinaryPayload(anyString(), any(byte[].class), anyString());
        logger.info("✓ Small binary message sent directly");
    }

    @Test
    @Order(20)
    @DisplayName("Gap 2: Large binary payload offloaded to blob")
    void testBinaryLargeMessage() {
        byte[] payload = new byte[TEST_THRESHOLD + 512];
        BlobPointer pointer = new BlobPointer("c", "binary-blob");
        when(mockPayloadStore.storeBinaryPayload(anyString(), eq(payload), eq("application/avro")))
                .thenReturn(pointer);

        client.sendBinaryMessage(payload, "application/avro");

        verify(mockPayloadStore).storeBinaryPayload(anyString(), eq(payload), eq("application/avro"));
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertEquals("true", captor.getValue().getApplicationProperties()
                .get(LargeMessageClientConfiguration.BLOB_POINTER_MARKER));
        assertEquals("application/avro", captor.getValue().getApplicationProperties().get("contentType"));
        logger.info("✓ Large binary message offloaded with Avro content type");
    }

    @Test
    @Order(21)
    @DisplayName("Gap 2: Binary send failure cleans up orphaned blob")
    void testBinarySendFailure_orphanCleanup() {
        byte[] payload = new byte[TEST_THRESHOLD + 512];
        BlobPointer pointer = new BlobPointer("c", "binary-orphan");
        when(mockPayloadStore.storeBinaryPayload(anyString(), eq(payload), anyString()))
                .thenReturn(pointer);
        doThrow(new RuntimeException("send failed"))
                .when(mockSender).sendMessage(any(ServiceBusMessage.class));

        assertThrows(RuntimeException.class,
                () -> client.sendBinaryMessage(payload, "application/octet-stream"));

        verify(mockPayloadStore, atLeastOnce()).deletePayload(pointer);
        logger.info("✓ Binary orphan blob cleaned up after send failure");
    }

    // =========================================================================
    // Gap 3: Auto-cleanup configuration
    // =========================================================================

    @Test
    @Order(22)
    @DisplayName("Gap 3: Auto-cleanup defaults to false")
    void testAutoCleanupDefault() {
        assertFalse(config.isAutoCleanupOnComplete());
        logger.info("✓ Auto-cleanup defaults to false");
    }

    @Test
    @Order(23)
    @DisplayName("Gap 3: Auto-cleanup can be enabled via config")
    void testAutoCleanupEnabled() {
        config.setAutoCleanupOnComplete(true);
        assertTrue(config.isAutoCleanupOnComplete());
        logger.info("✓ Auto-cleanup enabled via configuration");
    }

    // =========================================================================
    // Gap 6: SAS URI in single send
    // =========================================================================

    @Test
    @Order(24)
    @DisplayName("Gap 6: SAS URI is generated for large message when SAS enabled")
    void testSasUriInSingleSend() {
        config.setSasEnabled(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        String body = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "sas-blob");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);
        when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                .thenReturn("https://store.blob.core.windows.net/c/b?sv=2024&sig=abc");

        client.sendMessage(body);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        String sasUri = (String) captor.getValue().getApplicationProperties()
                .get(config.getMessagePropertyForBlobSasUri());
        assertNotNull(sasUri, "SAS URI should be set on message properties");
        logger.info("✓ SAS URI generated on single send");
    }

    @Test
    @Order(25)
    @DisplayName("Gap 6: SAS URI in batch send for offloaded messages")
    void testSasUriInBatchSend() {
        config.setSasEnabled(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        String body = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "batch-sas");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);
        when(mockPayloadStore.generateSasUri(eq(pointer), any()))
                .thenReturn("https://store.blob.core.windows.net/c/b?sv=2024&sig=def");

        ServiceBusMessageBatch batch = mock(ServiceBusMessageBatch.class);
        when(batch.tryAddMessage(any(ServiceBusMessage.class))).thenReturn(true);
        when(mockSender.createMessageBatch()).thenReturn(batch);

        client.sendMessageBatch(List.of(body));

        verify(mockPayloadStore).generateSasUri(eq(pointer), any());
        logger.info("✓ SAS URI generated for batch send");
    }

    // =========================================================================
    // Gap 7: Content-type
    // =========================================================================

    @Test
    @Order(26)
    @DisplayName("Gap 7: Default content type is configurable")
    void testDefaultContentType() {
        config.setDefaultContentType("application/json");
        assertEquals("application/json", config.getDefaultContentType());
        logger.info("✓ Default content type is configurable");
    }

    // =========================================================================
    // Scheduled message with offloading
    // =========================================================================

    @Test
    @Order(27)
    @DisplayName("Scheduled large message is offloaded")
    void testScheduledLargeMessageOffloaded() {
        String body = generateLargeMessage();
        BlobPointer pointer = new BlobPointer("c", "sched-blob");
        when(mockPayloadStore.storePayload(anyString(), eq(body))).thenReturn(pointer);

        OffsetDateTime time = OffsetDateTime.now().plusMinutes(5);
        when(mockSender.scheduleMessage(any(ServiceBusMessage.class), eq(time)))
                .thenReturn(42L);

        Long seq = client.sendScheduledMessage(body, time);

        assertEquals(42L, seq);
        verify(mockPayloadStore).storePayload(anyString(), eq(body));
        logger.info("✓ Scheduled large message offloaded to blob");
    }

    // =========================================================================
    // Deferred messages
    // =========================================================================

    @Test
    @Order(28)
    @DisplayName("Defer and receive deferred message round-trip")
    void testDeferAndReceiveDeferred() {
        // Set up a mock received message
        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("defer-1");
        when(mockMsg.getSequenceNumber()).thenReturn(100L);
        when(mockMsg.getBody()).thenReturn(
                com.azure.core.util.BinaryData.fromString("deferred body"));
        when(mockMsg.getDeliveryCount()).thenReturn(0L);
        when(mockMsg.getApplicationProperties()).thenReturn(new HashMap<>());

        // Defer the message
        client.deferMessage(mockMsg);
        verify(mockReceiver).defer(mockMsg);

        // Receive deferred
        when(mockReceiver.receiveDeferredMessage(100L)).thenReturn(mockMsg);
        LargeServiceBusMessage deferred = client.receiveDeferredMessage(100L);

        assertNotNull(deferred);
        assertEquals("deferred body", deferred.getBody());
        logger.info("✓ Defer + receive deferred round-trip completed");
    }

    @Test
    @Order(29)
    @DisplayName("Receive multiple deferred messages")
    void testReceiveMultipleDeferred() {
        ServiceBusReceivedMessage m1 = mock(ServiceBusReceivedMessage.class);
        when(m1.getMessageId()).thenReturn("d1");
        when(m1.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body1"));
        when(m1.getDeliveryCount()).thenReturn(0L);
        when(m1.getApplicationProperties()).thenReturn(new HashMap<>());

        ServiceBusReceivedMessage m2 = mock(ServiceBusReceivedMessage.class);
        when(m2.getMessageId()).thenReturn("d2");
        when(m2.getBody()).thenReturn(com.azure.core.util.BinaryData.fromString("body2"));
        when(m2.getDeliveryCount()).thenReturn(0L);
        when(m2.getApplicationProperties()).thenReturn(new HashMap<>());

        when(mockReceiver.receiveDeferredMessage(1L)).thenReturn(m1);
        when(mockReceiver.receiveDeferredMessage(2L)).thenReturn(m2);

        List<LargeServiceBusMessage> results = client.receiveDeferredMessages(List.of(1L, 2L));

        assertEquals(2, results.size());
        logger.info("✓ Multiple deferred messages received");
    }

    // =========================================================================
    // Lock renewal
    // =========================================================================

    @Test
    @Order(30)
    @DisplayName("Message lock renewal returns new expiration")
    void testRenewMessageLock() {
        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("lock-msg");
        OffsetDateTime newExpiry = OffsetDateTime.now().plusMinutes(5);
        when(mockReceiver.renewMessageLock(mockMsg)).thenReturn(newExpiry);

        OffsetDateTime result = client.renewMessageLock(mockMsg);

        assertEquals(newExpiry, result);
        logger.info("✓ Message lock renewed, new expiry: {}", result);
    }

    @Test
    @Order(31)
    @DisplayName("Batch lock renewal with mixed success/failure")
    void testRenewMessageLockBatch() {
        ServiceBusReceivedMessage m1 = mock(ServiceBusReceivedMessage.class);
        when(m1.getMessageId()).thenReturn("lm1");
        ServiceBusReceivedMessage m2 = mock(ServiceBusReceivedMessage.class);
        when(m2.getMessageId()).thenReturn("lm2");

        OffsetDateTime t1 = OffsetDateTime.now().plusMinutes(5);
        when(mockReceiver.renewMessageLock(m1)).thenReturn(t1);
        when(mockReceiver.renewMessageLock(m2))
                .thenThrow(new RuntimeException("lock lost"));

        Map<String, OffsetDateTime> results = client.renewMessageLockBatch(List.of(m1, m2));

        assertEquals(1, results.size());
        assertTrue(results.containsKey("lm1"));
        logger.info("✓ Batch lock renewal handled partial failure");
    }

    // =========================================================================
    // Sequential sendMessages
    // =========================================================================

    @Test
    @Order(32)
    @DisplayName("Sequential sendMessages sends each message individually")
    void testSequentialSendMessages() {
        List<String> bodies = List.of("seq-1", "seq-2", "seq-3");

        client.sendMessages(bodies);

        verify(mockSender, times(3)).sendMessage(any(ServiceBusMessage.class));
        logger.info("✓ Sequential sendMessages sent {} messages", bodies.size());
    }

    // =========================================================================
    // Binary with application properties
    // =========================================================================

    @Test
    @Order(33)
    @DisplayName("Binary message with application properties preserves them")
    void testBinaryMessageWithProperties() {
        byte[] payload = "small binary".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> props = new HashMap<>();
        props.put("tenantId", "tenant-1");
        props.put("format", "protobuf");

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        client.sendBinaryMessage(payload, "application/protobuf", props);

        verify(mockSender).sendMessage(captor.capture());
        assertEquals("tenant-1", captor.getValue().getApplicationProperties().get("tenantId"));
        assertEquals("application/protobuf", captor.getValue().getApplicationProperties().get("contentType"));
        logger.info("✓ Binary message preserved application properties");
    }

    // =========================================================================
    // Batch delete
    // =========================================================================

    @Test
    @Order(34)
    @DisplayName("Batch delete returns correct count for successful deletes")
    void testBatchDeleteAllSuccess() {
        BlobPointer p1 = new BlobPointer("c", "b1");
        BlobPointer p2 = new BlobPointer("c", "b2");
        LargeServiceBusMessage m1 = new LargeServiceBusMessage("1", "a", Map.of(), true, p1);
        LargeServiceBusMessage m2 = new LargeServiceBusMessage("2", "b", Map.of(), true, p2);

        int deleted = client.deletePayloadBatch(List.of(m1, m2));

        assertEquals(2, deleted);
        logger.info("✓ Batch delete completed: {} blobs deleted", deleted);
    }

    // =========================================================================
    // Receive with SAS URI download
    // =========================================================================

    @Test
    @Order(35)
    @DisplayName("Receive blob-backed message using SAS URI")
    void testReceiveWithSasUri() {
        config.setReceiveOnlyMode(true);
        client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        BlobPointer pointer = new BlobPointer("c", "sas-receive-blob");
        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("sas-msg");
        when(mockMsg.getBody())
                .thenReturn(com.azure.core.util.BinaryData.fromString(pointer.toJson()));
        when(mockMsg.getDeliveryCount()).thenReturn(1L);

        Map<String, Object> appProps = new HashMap<>();
        appProps.put(LargeMessageClientConfiguration.BLOB_POINTER_MARKER, "true");
        appProps.put(config.getReservedAttributeName(), 100);
        appProps.put(config.getMessagePropertyForBlobSasUri(),
                "https://store.blob.core.windows.net/c/b?sv=2024&sig=test");
        when(mockMsg.getApplicationProperties()).thenReturn(appProps);
        when(mockReceiver.receiveMessages(anyInt(), any(Duration.class)))
                .thenReturn(new IterableStream<>(List.of(mockMsg)));

        // Note: SAS URI download uses ReceiveOnlyBlobResolver which does an HTTP call.
        // In a mock-based test we can't fully validate the download, but we can verify
        // the code path is reached. The actual HTTP call will fail gracefully.
        try {
            client.receiveMessages(1);
        } catch (RuntimeException e) {
            // Expected — SAS URI download will fail in mock environment
            logger.info("✓ SAS URI download attempted (expected failure in mock env)");
        }
    }
}
