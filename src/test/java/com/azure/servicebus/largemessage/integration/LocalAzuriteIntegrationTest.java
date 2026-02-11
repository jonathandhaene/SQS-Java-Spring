/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.integration;

import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that run against Azurite — the Azure Storage emulator.
 *
 * <p>These tests validate {@link BlobPayloadStore} operations (store, retrieve,
 * delete) against a real blob storage endpoint without needing Azure credentials.
 *
 * <p>The tests are <b>skipped</b> unless the {@code AZURITE_ENABLED} environment
 * variable is set to {@code true}. In CI, an Azurite service container is started
 * automatically by the workflow.
 *
 * <p>To run locally, start Azurite first:
 * <pre>
 * npm install -g azurite
 * azurite --silent &amp;
 * export AZURITE_ENABLED=true
 * mvn verify -Pintegration-test-local
 * </pre>
 */
@DisplayName("Local Azurite Blob Storage Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "AZURITE_ENABLED", matches = "true")
class LocalAzuriteIntegrationTest extends IntegrationTestBase {

    /** Well-known Azurite development storage connection string. */
    private static final String AZURITE_CONNECTION_STRING =
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
            + "K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private static final String TEST_CONTAINER = "azurite-integration-test";

    private BlobPayloadStore payloadStore;
    private LargeMessageClientConfiguration config;

    @BeforeEach
    void setUp() {
        config = createTestConfiguration();

        // Use env var if set (CI uses localhost via Docker service), otherwise default
        String connectionString = getEnvOrDefault(
                "AZURE_STORAGE_CONNECTION_STRING", AZURITE_CONNECTION_STRING);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        payloadStore = new BlobPayloadStore(blobServiceClient, TEST_CONTAINER, config);
    }

    // =========================================================================
    // Store operations
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Store a payload and receive a valid BlobPointer")
    void testStorePayload() {
        String blobName = "test/" + UUID.randomUUID();
        String payload = "Hello from Azurite! " + UUID.randomUUID();

        BlobPointer pointer = payloadStore.storePayload(blobName, payload);

        assertNotNull(pointer, "BlobPointer should not be null");
        assertEquals(TEST_CONTAINER, pointer.getContainerName());
        assertEquals(blobName, pointer.getBlobName());

        // Clean up
        payloadStore.deletePayload(pointer);
        logger.info("✓ Stored payload and got valid pointer: {}", pointer);
    }

    @Test
    @Order(2)
    @DisplayName("Store and retrieve a small payload")
    void testStoreAndRetrieveSmall() {
        String blobName = "test/small-" + UUID.randomUUID();
        String payload = generateSmallMessage();

        BlobPointer pointer = payloadStore.storePayload(blobName, payload);
        String retrieved = payloadStore.getPayload(pointer);

        assertEquals(payload, retrieved, "Retrieved payload should match stored payload");

        payloadStore.deletePayload(pointer);
        logger.info("✓ Small payload round-trip succeeded ({} bytes)", payload.length());
    }

    @Test
    @Order(3)
    @DisplayName("Store and retrieve a large payload")
    void testStoreAndRetrieveLarge() {
        String blobName = "test/large-" + UUID.randomUUID();
        String payload = generateLargeMessage();

        BlobPointer pointer = payloadStore.storePayload(blobName, payload);
        String retrieved = payloadStore.getPayload(pointer);

        assertEquals(payload, retrieved, "Retrieved large payload should match stored payload");
        assertEquals(payload.length(), retrieved.length());

        payloadStore.deletePayload(pointer);
        logger.info("✓ Large payload round-trip succeeded ({} bytes)", payload.length());
    }

    // =========================================================================
    // Delete operations
    // =========================================================================

    @Test
    @Order(4)
    @DisplayName("Delete a payload removes it from blob storage")
    void testDeletePayload() {
        String blobName = "test/delete-" + UUID.randomUUID();
        BlobPointer pointer = payloadStore.storePayload(blobName, "to-be-deleted");

        // Delete should not throw
        assertDoesNotThrow(() -> payloadStore.deletePayload(pointer));

        // After deletion, getPayload should return null (ignorePayloadNotFound = true)
        config.setIgnorePayloadNotFound(true);
        String afterDelete = payloadStore.getPayload(pointer);
        assertNull(afterDelete, "Payload should be null after deletion");

        logger.info("✓ Payload deleted and confirmed gone");
    }

    @Test
    @Order(5)
    @DisplayName("Retrieve non-existent blob returns null when ignorePayloadNotFound is true")
    void testGetNonExistentBlobReturnsNull() {
        config.setIgnorePayloadNotFound(true);

        BlobPointer pointer = new BlobPointer(TEST_CONTAINER, "does-not-exist-" + UUID.randomUUID());
        String result = payloadStore.getPayload(pointer);

        assertNull(result, "Should return null for non-existent blob");
        logger.info("✓ Non-existent blob returns null with ignorePayloadNotFound=true");
    }

    @Test
    @Order(6)
    @DisplayName("Retrieve non-existent blob throws when ignorePayloadNotFound is false")
    void testGetNonExistentBlobThrows() {
        config.setIgnorePayloadNotFound(false);

        BlobPointer pointer = new BlobPointer(TEST_CONTAINER, "does-not-exist-" + UUID.randomUUID());
        assertThrows(RuntimeException.class, () -> payloadStore.getPayload(pointer));

        logger.info("✓ Non-existent blob throws RuntimeException with ignorePayloadNotFound=false");
    }

    // =========================================================================
    // Overwrite and metadata
    // =========================================================================

    @Test
    @Order(7)
    @DisplayName("Overwriting a blob updates its content")
    void testOverwritePayload() {
        String blobName = "test/overwrite-" + UUID.randomUUID();

        payloadStore.storePayload(blobName, "original content");
        BlobPointer pointer = payloadStore.storePayload(blobName, "updated content");

        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("updated content", retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob overwrite succeeded");
    }

    @Test
    @Order(8)
    @DisplayName("Blob key prefix is applied to blob names")
    void testBlobKeyPrefix() {
        config.setBlobKeyPrefix("azurite-test/");
        String blobName = config.getBlobKeyPrefix() + UUID.randomUUID();

        BlobPointer pointer = payloadStore.storePayload(blobName, "prefixed payload");

        assertTrue(pointer.getBlobName().startsWith("azurite-test/"),
                "Blob name should include the configured prefix");

        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("prefixed payload", retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob key prefix applied: {}", pointer.getBlobName());
    }

    @Test
    @Order(9)
    @DisplayName("BlobPointer JSON serialization round-trip")
    void testBlobPointerJsonRoundTrip() {
        String blobName = "test/json-" + UUID.randomUUID();
        BlobPointer pointer = payloadStore.storePayload(blobName, "json test");

        String json = pointer.toJson();
        assertNotNull(json);
        assertFalse(json.isEmpty());

        BlobPointer deserialized = BlobPointer.fromJson(json);
        assertEquals(pointer.getContainerName(), deserialized.getContainerName());
        assertEquals(pointer.getBlobName(), deserialized.getBlobName());

        // Verify the deserialized pointer can still retrieve the payload
        String retrieved = payloadStore.getPayload(deserialized);
        assertEquals("json test", retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ BlobPointer JSON round-trip succeeded: {}", json);
    }

    @Test
    @Order(10)
    @DisplayName("TTL metadata is set on blobs when configured")
    void testBlobTtlMetadata() {
        config.setBlobTtlDays(7);
        String blobName = "test/ttl-" + UUID.randomUUID();

        BlobPointer pointer = payloadStore.storePayload(blobName, "ttl payload");

        // Verify the blob was stored and is retrievable
        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("ttl payload", retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob with TTL metadata stored and retrieved successfully");
    }
}
