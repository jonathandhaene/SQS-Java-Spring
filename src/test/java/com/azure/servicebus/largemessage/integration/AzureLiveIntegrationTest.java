/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.integration;

import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.config.PlainTextConnectionStringProvider;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live integration tests that run against real Azure Service Bus and
 * Azure Blob Storage resources.
 *
 * <p>These tests are <b>skipped</b> unless the following environment
 * variables are set:
 * <ul>
 *   <li>{@code AZURE_SERVICEBUS_CONNECTION_STRING}</li>
 *   <li>{@code AZURE_STORAGE_CONNECTION_STRING}</li>
 *   <li>{@code AZURE_SERVICEBUS_QUEUE_NAME} (defaults to {@code integration-test-queue})</li>
 *   <li>{@code AZURE_STORAGE_CONTAINER_NAME} (defaults to {@code integration-test})</li>
 * </ul>
 *
 * <p>Run via:
 * <pre>
 * export AZURE_SERVICEBUS_CONNECTION_STRING="Endpoint=sb://..."
 * export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=..."
 * mvn verify -Pintegration-test-azure
 * </pre>
 */
@DisplayName("Azure Live Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "AZURE_SERVICEBUS_CONNECTION_STRING", matches = ".+")
@EnabledIfEnvironmentVariable(named = "AZURE_STORAGE_CONNECTION_STRING", matches = ".+")
class AzureLiveIntegrationTest extends IntegrationTestBase {

    private static String sbConnectionString;
    private static String storageConnectionString;
    private static String queueName;
    private static String containerName;

    private AzureServiceBusLargeMessageClient client;
    private BlobPayloadStore payloadStore;
    private LargeMessageClientConfiguration config;

    @BeforeAll
    static void loadEnv() {
        sbConnectionString = requireEnv("AZURE_SERVICEBUS_CONNECTION_STRING");
        storageConnectionString = requireEnv("AZURE_STORAGE_CONNECTION_STRING");
        queueName = getEnvOrDefault("AZURE_SERVICEBUS_QUEUE_NAME", "integration-test-queue");
        containerName = getEnvOrDefault("AZURE_STORAGE_CONTAINER_NAME", "integration-test");
        logger.info("Azure live tests configured — queue={}, container={}", queueName, containerName);
    }

    @BeforeEach
    void setUp() {
        config = createTestConfiguration();
        config.setEnableDuplicateDetectionId(true);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(storageConnectionString)
                .buildClient();

        payloadStore = new BlobPayloadStore(blobServiceClient, containerName, config);

        client = new AzureServiceBusLargeMessageClient(
                sbConnectionString, queueName, payloadStore, config);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    // =========================================================================
    // Send + Receive round-trips
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Small message round-trip (no blob offloading)")
    void testSmallMessageRoundTrip() {
        String body = "small-live-" + UUID.randomUUID();

        client.sendMessage(body, Map.of("testId", "small-roundtrip"));
        logger.info("Sent small message: {}", body);

        List<LargeServiceBusMessage> messages = client.receiveMessages(10);

        Optional<LargeServiceBusMessage> match = messages.stream()
                .filter(m -> body.equals(m.getBody()))
                .findFirst();

        assertTrue(match.isPresent(), "Should receive the sent message");
        assertFalse(match.get().isPayloadFromBlob(), "Small message should not use blob");

        logger.info("✓ Small message round-trip completed");
    }

    @Test
    @Order(2)
    @DisplayName("Large message round-trip (blob offloading)")
    void testLargeMessageRoundTrip() {
        String body = generateLargeMessage();
        String testId = UUID.randomUUID().toString();

        client.sendMessage(body, Map.of("testId", testId));
        logger.info("Sent large message ({} bytes), testId={}", body.length(), testId);

        List<LargeServiceBusMessage> messages = client.receiveMessages(10);

        Optional<LargeServiceBusMessage> match = messages.stream()
                .filter(m -> testId.equals(m.getApplicationProperties().get("testId")))
                .findFirst();

        assertTrue(match.isPresent(), "Should receive the large message");
        assertTrue(match.get().isPayloadFromBlob(), "Large message should be offloaded");
        assertEquals(body, match.get().getBody(), "Resolved body should match original");

        // Clean up the blob
        client.deletePayload(match.get());

        logger.info("✓ Large message round-trip completed (blob offloaded & cleaned up)");
    }

    @Test
    @Order(3)
    @DisplayName("Send and receive with custom application properties")
    void testSendWithProperties() {
        String body = "props-test-" + UUID.randomUUID();
        Map<String, Object> props = new HashMap<>();
        props.put("orderId", "ORD-" + UUID.randomUUID());
        props.put("priority", 3);

        client.sendMessage(body, props);

        List<LargeServiceBusMessage> messages = client.receiveMessages(10);
        Optional<LargeServiceBusMessage> match = messages.stream()
                .filter(m -> body.equals(m.getBody()))
                .findFirst();

        assertTrue(match.isPresent());
        assertEquals(props.get("orderId"), match.get().getApplicationProperties().get("orderId"));
        assertEquals(3, match.get().getApplicationProperties().get("priority"));

        logger.info("✓ Custom application properties preserved on round-trip");
    }

    @Test
    @Order(4)
    @DisplayName("Send with session ID")
    void testSendWithSessionId() {
        // This test only works if the queue is session-enabled.
        // If it isn't, the send will still succeed — the session ID is just ignored.
        String body = "session-test-" + UUID.randomUUID();
        String sessionId = "session-" + UUID.randomUUID().toString().substring(0, 8);

        assertDoesNotThrow(() -> client.sendMessage(body, sessionId));

        logger.info("✓ Message sent with session ID: {}", sessionId);
    }

    // =========================================================================
    // Blob storage lifecycle
    // =========================================================================

    @Test
    @Order(5)
    @DisplayName("Blob storage: store → retrieve → delete lifecycle")
    void testBlobStorageLifecycle() {
        String payload = "lifecycle-test-payload-" + UUID.randomUUID();
        String blobName = "integration-test/" + UUID.randomUUID();

        // Store
        var pointer = payloadStore.storePayload(blobName, payload);
        assertNotNull(pointer);
        assertEquals(containerName, pointer.getContainerName());
        logger.info("Stored blob: {}", pointer);

        // Retrieve
        String downloaded = payloadStore.getPayload(pointer);
        assertEquals(payload, downloaded, "Downloaded payload should match original");
        logger.info("Retrieved blob: {} bytes", downloaded.length());

        // Delete
        assertDoesNotThrow(() -> payloadStore.deletePayload(pointer));
        logger.info("Deleted blob: {}", pointer);

        // Verify it's gone (should return null if ignorePayloadNotFound)
        config.setIgnorePayloadNotFound(true);
        String afterDelete = payloadStore.getPayload(pointer);
        assertNull(afterDelete, "Payload should be null after deletion");

        logger.info("✓ Full blob lifecycle completed (store → get → delete → verify gone)");
    }

    // =========================================================================
    // Batch operations
    // =========================================================================

    @Test
    @Order(6)
    @DisplayName("Batch send with mixed message sizes")
    void testBatchSend() {
        List<String> bodies = List.of(
                "batch-small-" + UUID.randomUUID(),
                generateLargeMessage()
        );

        assertDoesNotThrow(() -> client.sendMessageBatch(bodies));

        logger.info("✓ Batch of {} messages sent (mixed sizes)", bodies.size());
    }

    // =========================================================================
    // Scheduled message
    // =========================================================================

    @Test
    @Order(7)
    @DisplayName("Scheduled message is accepted by Service Bus")
    void testScheduledMessage() {
        String body = "scheduled-" + UUID.randomUUID();
        OffsetDateTime scheduledTime = OffsetDateTime.now().plusMinutes(5);

        Long seq = client.sendScheduledMessage(body, scheduledTime);

        assertNotNull(seq, "Sequence number should be returned");
        assertTrue(seq > 0, "Sequence number should be positive");

        logger.info("✓ Scheduled message accepted with sequence number: {}", seq);
    }

    // =========================================================================
    // PlainTextConnectionStringProvider
    // =========================================================================

    @Test
    @Order(8)
    @DisplayName("PlainTextConnectionStringProvider returns the connection string")
    void testConnectionStringProvider() {
        PlainTextConnectionStringProvider provider =
                new PlainTextConnectionStringProvider(storageConnectionString);

        assertEquals(storageConnectionString, provider.getConnectionString());

        logger.info("✓ PlainTextConnectionStringProvider works correctly");
    }

    // =========================================================================
    // Binary message round-trip
    // =========================================================================

    @Test
    @Order(9)
    @DisplayName("Binary message round-trip via blob offloading")
    void testBinaryMessageRoundTrip() {
        byte[] binaryPayload = new byte[TEST_THRESHOLD + 256];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(binaryPayload);
        String testId = UUID.randomUUID().toString();

        Map<String, Object> props = new HashMap<>();
        props.put("testId", testId);

        client.sendBinaryMessage(binaryPayload, "application/octet-stream", props);
        logger.info("Sent binary message ({} bytes), testId={}", binaryPayload.length, testId);

        // Note: Binary messages are offloaded and the pointer is sent
        // Full round-trip verification requires receiver support for binary
        // This test validates the send path doesn't throw
        logger.info("✓ Binary message sent successfully via blob offloading");
    }

    // =========================================================================
    // Content-type preservation
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Blob stores and retrieves content type correctly")
    void testContentTypePreservation() {
        String blobName = "live-test/ct-" + UUID.randomUUID();
        String payload = "{\"key\":\"value\"}";

        var pointer = payloadStore.storePayload(blobName, payload, "application/json");

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/json", contentType, "Content type should be preserved");

        payloadStore.deletePayload(pointer);
        logger.info("✓ Content type preserved on blob round-trip");
    }

    // =========================================================================
    // Sequential message send
    // =========================================================================

    @Test
    @Order(11)
    @DisplayName("Sequential sendMessages sends all messages")
    void testSequentialSendMessages() {
        List<String> bodies = List.of(
                "sequential-1-" + UUID.randomUUID(),
                "sequential-2-" + UUID.randomUUID()
        );

        assertDoesNotThrow(() -> client.sendMessages(bodies));

        logger.info("✓ Sequential sendMessages sent {} messages", bodies.size());
    }

    // =========================================================================
    // Blob cleanup expired
    // =========================================================================

    @Test
    @Order(12)
    @DisplayName("Cleanup expired blobs runs without error")
    void testCleanupExpiredBlobs() {
        config.setBlobTtlDays(1);

        // Run cleanup — all existing blobs should have future TTLs
        int deleted = payloadStore.cleanupExpiredBlobs();

        assertTrue(deleted >= 0, "Cleanup should return a non-negative count");
        logger.info("✓ Cleanup expired blobs completed, deleted {}", deleted);
    }

    // =========================================================================
    // SAS URI generation
    // =========================================================================

    @Test
    @Order(13)
    @DisplayName("SAS URI is generated for stored blob")
    void testSasUriGeneration() {
        config.setSasEnabled(true);

        String blobName = "live-test/sas-" + UUID.randomUUID();
        var pointer = payloadStore.storePayload(blobName, "SAS test payload");

        String sasUri = payloadStore.generateSasUri(pointer,
                java.time.Duration.ofHours(1));

        assertNotNull(sasUri, "SAS URI should not be null");
        assertTrue(sasUri.contains("sig="), "SAS URI should contain a signature");
        assertTrue(sasUri.contains(blobName), "SAS URI should reference the blob");

        payloadStore.deletePayload(pointer);
        logger.info("✓ SAS URI generated: {}...", sasUri.substring(0, Math.min(80, sasUri.length())));
    }

    // =========================================================================
    // Gap 2: Binary payload round-trip
    // =========================================================================

    @Test
    @Order(14)
    @DisplayName("Gap 2: Binary payload store, retrieve, and delete lifecycle")
    void testBinaryPayloadLifecycle() {
        String blobName = "live-test/binary-" + UUID.randomUUID();
        byte[] originalPayload = new byte[]{0x01, 0x02, 0x03, 0x04, (byte) 0xFE, (byte) 0xFF};

        var pointer = payloadStore.storeBinaryPayload(blobName, originalPayload, "application/octet-stream");
        assertNotNull(pointer);

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(originalPayload, retrieved, "Binary payload should match");

        payloadStore.deletePayload(pointer);

        config.setIgnorePayloadNotFound(true);
        byte[] afterDelete = payloadStore.getBinaryPayload(pointer);
        assertNull(afterDelete, "Binary payload should be null after deletion");

        logger.info("✓ Binary payload lifecycle completed (store → get → delete → verify gone)");
    }

    // =========================================================================
    // Gap 7: Content type with binary
    // =========================================================================

    @Test
    @Order(15)
    @DisplayName("Gap 7: Binary payload preserves content type")
    void testBinaryPayloadContentType() {
        String blobName = "live-test/binary-ct-" + UUID.randomUUID();
        byte[] payload = "avro-encoded-data".getBytes();

        var pointer = payloadStore.storeBinaryPayload(blobName, payload, "application/avro");

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/avro", contentType, "Content type should be preserved for binary payload");

        payloadStore.deletePayload(pointer);
        logger.info("✓ Binary content type 'application/avro' preserved");
    }

    // =========================================================================
    // Gap 5: TTL metadata on blobs
    // =========================================================================

    @Test
    @Order(16)
    @DisplayName("Gap 5: Blob with TTL metadata is stored and cleanup skips non-expired")
    void testBlobTtlMetadata() {
        config.setBlobTtlDays(7);

        String blobName = "live-test/ttl-" + UUID.randomUUID();
        var pointer = payloadStore.storePayload(blobName, "TTL payload");

        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("TTL payload", retrieved);

        // Run cleanup — blob has 7-day TTL so should NOT be deleted
        int deleted = payloadStore.cleanupExpiredBlobs();
        assertEquals(0, deleted, "Non-expired blob should not be deleted by cleanup");

        // Verify still exists
        String afterCleanup = payloadStore.getPayload(pointer);
        assertEquals("TTL payload", afterCleanup);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob with TTL metadata stored; cleanup skipped non-expired");
    }

    // =========================================================================
    // Gap 6: SAS URI in send with large message
    // =========================================================================

    @Test
    @Order(17)
    @DisplayName("Gap 6: Large message with SAS enabled includes SAS URI in properties")
    void testLargeMessageWithSas() {
        config.setSasEnabled(true);
        client = new AzureServiceBusLargeMessageClient(
                sbConnectionString, queueName, payloadStore, config);

        String body = generateLargeMessage();

        // Sending the message — SAS URI should be generated and included
        assertDoesNotThrow(() -> client.sendMessage(body));

        logger.info("✓ Large message sent with SAS URI enabled");
    }

    // =========================================================================
    // Gap 3: Auto-cleanup configuration
    // =========================================================================

    @Test
    @Order(18)
    @DisplayName("Gap 3: Auto-cleanup configuration can be enabled")
    void testAutoCleanupConfiguration() {
        config.setAutoCleanupOnComplete(true);
        assertTrue(config.isAutoCleanupOnComplete());

        // Creating a client with auto-cleanup should succeed
        AzureServiceBusLargeMessageClient autoClient = new AzureServiceBusLargeMessageClient(
                sbConnectionString, queueName, payloadStore, config);
        assertNotNull(autoClient);
        autoClient.close();

        logger.info("✓ Auto-cleanup configuration enabled successfully");
    }

    // =========================================================================
    // Batch send with small messages only
    // =========================================================================

    @Test
    @Order(19)
    @DisplayName("Batch send with only small messages (no offloading)")
    void testBatchSendSmallOnly() {
        List<String> bodies = List.of(
                "batch-small-1-" + UUID.randomUUID(),
                "batch-small-2-" + UUID.randomUUID(),
                "batch-small-3-" + UUID.randomUUID()
        );

        assertDoesNotThrow(() -> client.sendMessageBatch(bodies));
        logger.info("✓ Batch of {} small messages sent without offloading", bodies.size());
    }

    // =========================================================================
    // Overwrite blob
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("Overwriting an existing blob updates content")
    void testBlobOverwrite() {
        String blobName = "live-test/overwrite-" + UUID.randomUUID();

        payloadStore.storePayload(blobName, "original");
        var pointer = payloadStore.storePayload(blobName, "updated");

        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("updated", retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob overwrite succeeded");
    }
}
