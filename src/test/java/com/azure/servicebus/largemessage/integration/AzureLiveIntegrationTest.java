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
}
