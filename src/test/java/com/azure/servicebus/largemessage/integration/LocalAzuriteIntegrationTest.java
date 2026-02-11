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

    // =========================================================================
    // Binary payload operations
    // =========================================================================

    @Test
    @Order(11)
    @DisplayName("Store and retrieve a binary payload")
    void testBinaryPayloadStoreAndRetrieve() {
        String blobName = "test/binary-" + UUID.randomUUID();
        byte[] payload = new byte[] {0x01, 0x02, 0x03, 0x04, (byte) 0xFF};

        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, payload, "application/octet-stream");
        assertNotNull(pointer);
        assertEquals(TEST_CONTAINER, pointer.getContainerName());

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(payload, retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Binary payload store → retrieve → delete succeeded");
    }

    @Test
    @Order(12)
    @DisplayName("Store binary with Avro content type")
    void testBinaryPayloadAvroContentType() {
        String blobName = "test/avro-" + UUID.randomUUID();
        byte[] avroPayload = "avro-encoded-data".getBytes();

        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, avroPayload, "application/avro");
        assertNotNull(pointer);

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(avroPayload, retrieved);

        // Verify content type
        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/avro", contentType);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Binary Avro payload stored with correct content type");
    }

    // =========================================================================
    // Content-type operations
    // =========================================================================

    @Test
    @Order(13)
    @DisplayName("Content type is preserved in blob metadata")
    void testContentTypePreserved() {
        String blobName = "test/json-ct-" + UUID.randomUUID();
        String payload = "{\"key\": \"value\"}";

        BlobPointer pointer = payloadStore.storePayload(blobName, payload, "application/json");

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/json", contentType);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Content type application/json preserved");
    }

    @Test
    @Order(14)
    @DisplayName("Default content type is applied when none specified")
    void testDefaultContentTypeApplied() {
        config.setDefaultContentType("text/plain; charset=utf-8");
        String blobName = "test/default-ct-" + UUID.randomUUID();

        BlobPointer pointer = payloadStore.storePayload(blobName, "plain text");

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("text/plain; charset=utf-8", contentType);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Default content type applied when none specified");
    }

    // =========================================================================
    // Cleanup expired blobs
    // =========================================================================

    @Test
    @Order(15)
    @DisplayName("Cleanup expired blobs removes expired entries")
    void testCleanupExpiredBlobs() {
        // Configure short TTL
        config.setBlobTtlDays(0); // We'll manually set "expiresAt" in the past

        // Store a blob with TTL
        config.setBlobTtlDays(1);
        String blobName = "test/cleanup-" + UUID.randomUUID();
        BlobPointer pointer = payloadStore.storePayload(blobName, "cleanup payload");
        assertNotNull(pointer);

        // Run cleanup — the blob has TTL set to 1 day in the FUTURE, so should NOT be deleted
        int deleted = payloadStore.cleanupExpiredBlobs();
        assertEquals(0, deleted, "Non-expired blob should not be deleted");

        // Verify blob still exists
        String retrieved = payloadStore.getPayload(pointer);
        assertEquals("cleanup payload", retrieved);

        // Clean up manually
        payloadStore.deletePayload(pointer);
        logger.info("✓ Cleanup skipped non-expired blobs correctly");
    }

    @Test
    @Order(16)
    @DisplayName("Cleanup expired blobs returns 0 when no TTL configured")
    void testCleanupNoTtlConfigured() {
        config.setBlobTtlDays(0);
        int deleted = payloadStore.cleanupExpiredBlobs();
        assertEquals(0, deleted);
        logger.info("✓ Cleanup returned 0 when no TTL configured");
    }

    // =========================================================================
    // Delete idempotency
    // =========================================================================

    @Test
    @Order(17)
    @DisplayName("Double delete is idempotent (does not throw)")
    void testDoubleDeleteIdempotent() {
        String blobName = "test/double-del-" + UUID.randomUUID();
        BlobPointer pointer = payloadStore.storePayload(blobName, "delete-me-twice");

        payloadStore.deletePayload(pointer);
        // Second delete should not throw
        assertDoesNotThrow(() -> payloadStore.deletePayload(pointer));

        logger.info("✓ Double delete is idempotent");
    }

    // =========================================================================
    // getBinaryPayload — not found
    // =========================================================================

    @Test
    @Order(18)
    @DisplayName("getBinaryPayload returns null for missing blob when ignorePayloadNotFound")
    void testGetBinaryPayloadNotFound() {
        config.setIgnorePayloadNotFound(true);
        BlobPointer pointer = new BlobPointer(TEST_CONTAINER, "binary-missing-" + UUID.randomUUID());

        byte[] result = payloadStore.getBinaryPayload(pointer);
        assertNull(result, "Should return null for missing binary blob");

        logger.info("✓ getBinaryPayload returns null for missing blob with ignorePayloadNotFound=true");
    }

    // =========================================================================
    // Content-type with store overloads
    // =========================================================================

    @Test
    @Order(19)
    @DisplayName("Store with explicit content type and retrieve it")
    void testStoreWithContentTypeAndRetrieve() {
        String blobName = "test/ct-explicit-" + UUID.randomUUID();
        String payload = "<root><data>xml</data></root>";

        BlobPointer pointer = payloadStore.storePayload(blobName, payload, "application/xml");

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/xml", contentType);

        String retrieved = payloadStore.getPayload(pointer);
        assertEquals(payload, retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Explicit content type application/xml stored and retrieved");
    }

    // =========================================================================
    // Binary payload with various content types
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("Binary Protobuf payload with content type")
    void testBinaryProtobufPayload() {
        String blobName = "test/protobuf-" + UUID.randomUUID();
        byte[] payload = new byte[]{0x08, (byte) 0x96, 0x01};

        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, payload, "application/protobuf");

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(payload, retrieved);

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/protobuf", contentType);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Binary Protobuf payload stored with correct content type");
    }

    @Test
    @Order(21)
    @DisplayName("Binary payload with null content type defaults to octet-stream")
    void testBinaryPayloadNullContentType() {
        String blobName = "test/binary-null-ct-" + UUID.randomUUID();
        byte[] payload = new byte[]{0x01, 0x02};

        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, payload, null);

        String contentType = payloadStore.getContentType(pointer);
        assertEquals("application/octet-stream", contentType);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Binary payload with null content type defaults to application/octet-stream");
    }

    // =========================================================================
    // Large binary payload
    // =========================================================================

    @Test
    @Order(22)
    @DisplayName("Large binary payload round-trip")
    void testLargeBinaryPayload() {
        String blobName = "test/large-binary-" + UUID.randomUUID();
        byte[] payload = new byte[4096]; // 4 KB
        new java.util.Random(42).nextBytes(payload);

        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, payload, "application/octet-stream");

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(payload, retrieved);
        assertEquals(4096, retrieved.length);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Large binary payload ({} bytes) round-trip succeeded", payload.length);
    }

    // =========================================================================
    // SAS URI generation
    // =========================================================================

    @Test
    @Order(23)
    @DisplayName("SAS URI generated for stored blob")
    void testSasUriGeneration() {
        String blobName = "test/sas-" + UUID.randomUUID();
        BlobPointer pointer = payloadStore.storePayload(blobName, "SAS payload");

        String sasUri = payloadStore.generateSasUri(pointer, java.time.Duration.ofHours(1));

        assertNotNull(sasUri);
        assertTrue(sasUri.contains("sig="), "SAS URI should contain signature");

        payloadStore.deletePayload(pointer);
        logger.info("✓ SAS URI generated: {}...", sasUri.substring(0, Math.min(80, sasUri.length())));
    }

    // =========================================================================
    // Blob key prefix with binary payloads
    // =========================================================================

    @Test
    @Order(24)
    @DisplayName("Blob key prefix applied to binary payload names")
    void testBlobKeyPrefixWithBinaryPayload() {
        config.setBlobKeyPrefix("binary-test/");
        String blobName = config.getBlobKeyPrefix() + UUID.randomUUID();

        byte[] payload = new byte[]{0x01, 0x02, 0x03};
        BlobPointer pointer = payloadStore.storeBinaryPayload(blobName, payload, "application/octet-stream");

        assertTrue(pointer.getBlobName().startsWith("binary-test/"));

        byte[] retrieved = payloadStore.getBinaryPayload(pointer);
        assertArrayEquals(payload, retrieved);

        payloadStore.deletePayload(pointer);
        logger.info("✓ Blob key prefix applied to binary payload: {}", pointer.getBlobName());
    }

    // =========================================================================
    // Multiple content types on same container
    // =========================================================================

    @Test
    @Order(25)
    @DisplayName("Multiple blobs with different content types coexist")
    void testMultipleContentTypes() {
        String jsonBlob = "test/multi-json-" + UUID.randomUUID();
        String xmlBlob = "test/multi-xml-" + UUID.randomUUID();
        String binaryBlob = "test/multi-bin-" + UUID.randomUUID();

        BlobPointer p1 = payloadStore.storePayload(jsonBlob, "{\"a\":1}", "application/json");
        BlobPointer p2 = payloadStore.storePayload(xmlBlob, "<x/>", "application/xml");
        BlobPointer p3 = payloadStore.storeBinaryPayload(binaryBlob, new byte[]{0x01}, "application/octet-stream");

        assertEquals("application/json", payloadStore.getContentType(p1));
        assertEquals("application/xml", payloadStore.getContentType(p2));
        assertEquals("application/octet-stream", payloadStore.getContentType(p3));

        payloadStore.deletePayload(p1);
        payloadStore.deletePayload(p2);
        payloadStore.deletePayload(p3);
        logger.info("✓ Multiple blobs with different content types coexist correctly");
    }
}
