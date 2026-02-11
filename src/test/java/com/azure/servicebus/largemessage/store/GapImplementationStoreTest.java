/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import com.azure.servicebus.largemessage.config.EncryptionConfiguration;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.core.http.rest.PagedIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.azure.storage.blob.options.BlobParallelUploadOptions;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for gap implementations in BlobPayloadStore:
 * - Content-type metadata storage and retrieval
 * - CustomerProvidedKey encryption application
 * - Binary payload storage and retrieval
 */
class GapImplementationStoreTest {

    private BlobServiceClient mockBlobServiceClient;
    private BlobContainerClient mockContainerClient;
    private BlobClient mockBlobClient;
    private LargeMessageClientConfiguration config;

    @BeforeEach
    void setUp() {
        mockBlobServiceClient = mock(BlobServiceClient.class);
        mockContainerClient = mock(BlobContainerClient.class);
        mockBlobClient = mock(BlobClient.class);

        when(mockBlobServiceClient.getBlobContainerClient(anyString())).thenReturn(mockContainerClient);
        when(mockContainerClient.exists()).thenReturn(true);
        when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient);
        when(mockBlobClient.getCustomerProvidedKeyClient(any())).thenReturn(mockBlobClient);

        config = new LargeMessageClientConfiguration();
    }

    // ========== Gap 7: Content-Type Metadata Tests ==========

    @Nested
    @DisplayName("Gap 7: Content-Type Metadata")
    class ContentTypeTests {

        @Test
        @DisplayName("Should store content type in blob metadata and HTTP headers")
        void testStorePayload_setsContentType() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            // Act
            store.storePayload("test-blob", "Hello, World!", "application/json");

            // Assert
            verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
            BlobParallelUploadOptions capturedOptions = optionsCaptor.getValue();
            
            // Verify HTTP headers
            assertNotNull(capturedOptions.getHeaders());
            assertEquals("application/json", capturedOptions.getHeaders().getContentType());
            
            // Verify metadata
            Map<String, String> metadata = capturedOptions.getMetadata();
            assertNotNull(metadata);
            assertEquals("application/json", metadata.get("contentType"));
        }

        @Test
        @DisplayName("Should use default content type when none is specified")
        void testStorePayload_defaultContentType() {
            // Arrange
            config.setDefaultContentType("text/plain; charset=utf-8");
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            // Act
            store.storePayload("test-blob", "Hello");

            // Assert
            verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
            BlobParallelUploadOptions capturedOptions = optionsCaptor.getValue();
            assertNotNull(capturedOptions.getHeaders());
            assertEquals("text/plain; charset=utf-8", capturedOptions.getHeaders().getContentType());
        }

        @Test
        @DisplayName("Should retrieve content type from blob properties")
        void testGetContentType() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobProperties mockProps = mock(BlobProperties.class);
            when(mockProps.getContentType()).thenReturn("application/json");
            when(mockBlobClient.getProperties()).thenReturn(mockProps);
            
            BlobPointer pointer = new BlobPointer("test-container", "test-blob");

            // Act
            String contentType = store.getContentType(pointer);

            // Assert
            assertEquals("application/json", contentType);
        }

        @Test
        @DisplayName("Should fall back to metadata for content type")
        void testGetContentType_fallsBackToMetadata() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobProperties mockProps = mock(BlobProperties.class);
            when(mockProps.getContentType()).thenReturn(null);
            Map<String, String> metadata = new HashMap<>();
            metadata.put("contentType", "application/xml");
            when(mockProps.getMetadata()).thenReturn(metadata);
            when(mockBlobClient.getProperties()).thenReturn(mockProps);
            
            BlobPointer pointer = new BlobPointer("test-container", "test-blob");

            // Act
            String contentType = store.getContentType(pointer);

            // Assert
            assertEquals("application/xml", contentType);
        }

        @Test
        @DisplayName("Should return null when content type is not available")
        void testGetContentType_returnsNullWhenNotSet() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobProperties mockProps = mock(BlobProperties.class);
            when(mockProps.getContentType()).thenReturn(null);
            when(mockProps.getMetadata()).thenReturn(new HashMap<>());
            when(mockBlobClient.getProperties()).thenReturn(mockProps);
            
            BlobPointer pointer = new BlobPointer("test-container", "test-blob");

            // Act
            String contentType = store.getContentType(pointer);

            // Assert
            assertNull(contentType);
        }
    }

    // ========== Gap 4: Encryption with CustomerProvidedKey Tests ==========

    @Nested
    @DisplayName("Gap 4: CustomerProvidedKey Encryption")
    class EncryptionTests {

        @Test
        @DisplayName("Should use CustomerProvidedKey client when CPK is configured")
        void testStorePayload_appliesCpk() {
            // Arrange
            EncryptionConfiguration encConfig = new EncryptionConfiguration();
            encConfig.setCustomerProvidedKey("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
            config.setEncryption(encConfig);
            
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            // Act
            store.storePayload("test-blob", "Encrypted payload");

            // Assert — verify getCustomerProvidedKeyClient was called (CPK applied)
            verify(mockBlobClient).getCustomerProvidedKeyClient(any());
        }

        @Test
        @DisplayName("Should not use CPK client when no key is configured")
        void testStorePayload_noCpkWhenNotConfigured() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            // Act
            store.storePayload("test-blob", "Unencrypted payload");

            // Assert — verify getCustomerProvidedKeyClient was NOT called
            verify(mockBlobClient, never()).getCustomerProvidedKeyClient(any());
        }

        @Test
        @DisplayName("Should set encryption scope in metadata")
        void testStorePayload_setsEncryptionScopeMetadata() {
            // Arrange
            EncryptionConfiguration encConfig = new EncryptionConfiguration();
            encConfig.setEncryptionScope("my-encryption-scope");
            config.setEncryption(encConfig);

            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            // Act
            store.storePayload("test-blob", "Payload with scope");

            // Assert
            verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
            Map<String, String> metadata = optionsCaptor.getValue().getMetadata();
            assertEquals("my-encryption-scope", metadata.get("encryptionScope"));
        }
    }

    // ========== Gap 2: Binary Payload Store Tests ==========

    @Nested
    @DisplayName("Gap 2: Binary Payload Storage")
    class BinaryPayloadStoreTests {

        @Test
        @DisplayName("Should store binary payload with content type")
        void testStoreBinaryPayload() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            byte[] payload = new byte[]{0x01, 0x02, 0x03, 0x04};

            // Act
            BlobPointer pointer = store.storeBinaryPayload("binary-blob", payload, "application/octet-stream");

            // Assert
            assertNotNull(pointer);
            assertEquals("test-container", pointer.getContainerName());
            assertEquals("binary-blob", pointer.getBlobName());
            verify(mockBlobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull());
        }

        @Test
        @DisplayName("Should store binary payload with Avro content type")
        void testStoreBinaryPayload_avro() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            byte[] avroPayload = "avro-data".getBytes();
            ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            // Act
            store.storeBinaryPayload("avro-blob", avroPayload, "application/avro");

            // Assert
            verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
            assertEquals("application/avro", optionsCaptor.getValue().getHeaders().getContentType());
        }

        @Test
        @DisplayName("Should use default content type for binary payload when null")
        void testStoreBinaryPayload_defaultContentType() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            byte[] payload = new byte[]{0x01};
            ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            // Act
            store.storeBinaryPayload("blob", payload, null);

            // Assert
            verify(mockBlobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
            assertEquals("application/octet-stream", optionsCaptor.getValue().getHeaders().getContentType());
        }

        @Test
        @DisplayName("Should retrieve binary payload from blob")
        void testGetBinaryPayload() {
            // Arrange
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            byte[] expectedData = new byte[]{0x01, 0x02, 0x03};
            when(mockBlobClient.downloadContent()).thenReturn(com.azure.core.util.BinaryData.fromBytes(expectedData));

            BlobPointer pointer = new BlobPointer("test-container", "binary-blob");

            // Act
            byte[] result = store.getBinaryPayload(pointer);

            // Assert
            assertArrayEquals(expectedData, result);
        }

        @Test
        @DisplayName("Should apply CPK to binary payload storage")
        void testStoreBinaryPayload_withCpk() {
            // Arrange
            EncryptionConfiguration encConfig = new EncryptionConfiguration();
            encConfig.setCustomerProvidedKey("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
            config.setEncryption(encConfig);

            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            byte[] payload = new byte[]{0x01, 0x02};

            // Act
            store.storeBinaryPayload("cpk-blob", payload, "application/octet-stream");

            // Assert
            verify(mockBlobClient).getCustomerProvidedKeyClient(any());
        }
    }

    // ========== Container Management Tests ==========

    @Nested
    @DisplayName("Container Management")
    class ContainerManagementTests {

        @Test
        @DisplayName("Should auto-create container when it does not exist")
        void testConstructor_createsContainerWhenNotExists() {
            when(mockContainerClient.exists()).thenReturn(false);

            new BlobPayloadStore(mockBlobServiceClient, "new-container", config);

            verify(mockContainerClient).create();
        }

        @Test
        @DisplayName("Should not create container when it already exists")
        void testConstructor_skipsCreateWhenExists() {
            // setUp already sets exists() to true
            new BlobPayloadStore(mockBlobServiceClient, "existing-container", config);

            verify(mockContainerClient, never()).create();
        }

        @Test
        @DisplayName("Should work with 2-param constructor (no config)")
        void testConstructor_twoParam() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container");

            // Should not throw, and should handle null config gracefully
            assertNotNull(store);
        }
    }

    // ========== Error Handling Tests ==========

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("getPayload throws RuntimeException for non-404 BlobStorageException")
        void testGetPayload_nonBlobNotFoundError_throwsRuntimeException() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "test-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.CONTAINER_NOT_FOUND);
            when(mockBlobClient.downloadContent()).thenThrow(exception);

            RuntimeException thrown = assertThrows(RuntimeException.class, () -> store.getPayload(pointer));
            assertTrue(thrown.getMessage().contains("Failed to retrieve payload"));
        }

        @Test
        @DisplayName("getPayload returns null for 404 when ignorePayloadNotFound is enabled")
        void testGetPayload_blobNotFound_withIgnore_returnsNull() {
            config.setIgnorePayloadNotFound(true);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "missing-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
            when(mockBlobClient.downloadContent()).thenThrow(exception);

            assertNull(store.getPayload(pointer));
        }

        @Test
        @DisplayName("getPayload throws for 404 when ignorePayloadNotFound is disabled")
        void testGetPayload_blobNotFound_withoutIgnore_throws() {
            config.setIgnorePayloadNotFound(false);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "missing-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
            when(mockBlobClient.downloadContent()).thenThrow(exception);

            assertThrows(RuntimeException.class, () -> store.getPayload(pointer));
        }

        @Test
        @DisplayName("getBinaryPayload throws RuntimeException for non-404 BlobStorageException")
        void testGetBinaryPayload_nonBlobNotFoundError_throws() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "binary-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.CONTAINER_NOT_FOUND);
            when(mockBlobClient.downloadContent()).thenThrow(exception);

            assertThrows(RuntimeException.class, () -> store.getBinaryPayload(pointer));
        }

        @Test
        @DisplayName("getBinaryPayload returns null for 404 when ignorePayloadNotFound is enabled")
        void testGetBinaryPayload_blobNotFound_withIgnore_returnsNull() {
            config.setIgnorePayloadNotFound(true);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "missing");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
            when(mockBlobClient.downloadContent()).thenThrow(exception);

            assertNull(store.getBinaryPayload(pointer));
        }

        @Test
        @DisplayName("deletePayload handles 404 gracefully (already deleted)")
        void testDeletePayload_blobNotFound_noThrow() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "gone-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
            doThrow(exception).when(mockBlobClient).delete();

            assertDoesNotThrow(() -> store.deletePayload(pointer));
        }

        @Test
        @DisplayName("deletePayload throws for non-404 BlobStorageException")
        void testDeletePayload_nonBlobNotFoundError_throws() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "err-blob");

            BlobStorageException exception = mock(BlobStorageException.class);
            when(exception.getErrorCode()).thenReturn(BlobErrorCode.CONTAINER_NOT_FOUND);
            doThrow(exception).when(mockBlobClient).delete();

            assertThrows(RuntimeException.class, () -> store.deletePayload(pointer));
        }

        @Test
        @DisplayName("storePayload wraps generic exceptions in RuntimeException")
        void testStorePayload_genericException_wrapsInRuntimeException() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull()))
                    .thenThrow(new RuntimeException("Upload failed"));

            assertThrows(RuntimeException.class, () -> store.storePayload("blob", "payload"));
        }

        @Test
        @DisplayName("storeBinaryPayload wraps exceptions in RuntimeException")
        void testStoreBinaryPayload_exception_wrapsInRuntimeException() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            when(mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull()))
                    .thenThrow(new RuntimeException("Binary upload failed"));

            assertThrows(RuntimeException.class,
                    () -> store.storeBinaryPayload("blob", new byte[]{0x01}, "application/octet-stream"));
        }
    }

    // ========== Access Tier Tests ==========

    @Nested
    @DisplayName("Access Tier")
    class AccessTierTests {

        @Test
        @DisplayName("Should set valid access tier on stored blob")
        void testStorePayload_validAccessTier() {
            config.setBlobAccessTier("Hot");
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            store.storePayload("blob", "payload");

            verify(mockBlobClient).setAccessTier(any(AccessTier.class));
        }

        @Test
        @DisplayName("Should handle setAccessTier failure gracefully")
        void testStorePayload_setAccessTierThrows_continuesGracefully() {
            config.setBlobAccessTier("SomeInvalidTier");
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            doThrow(new IllegalArgumentException("Invalid tier"))
                    .when(mockBlobClient).setAccessTier(any(AccessTier.class));

            // Should not throw — the error is caught and logged
            assertDoesNotThrow(() -> store.storePayload("blob", "payload"));
            verify(mockBlobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull());
        }

        @Test
        @DisplayName("Should not set access tier when not configured")
        void testStorePayload_noAccessTier() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            store.storePayload("blob", "payload");

            verify(mockBlobClient, never()).setAccessTier(any(AccessTier.class));
        }
    }

    // ========== TTL & Cleanup Tests ==========

    @Nested
    @DisplayName("TTL & Cleanup")
    class TtlCleanupTests {

        @Test
        @DisplayName("storePayload adds expiresAt metadata when TTL is configured")
        void testStorePayload_withTtl_addsExpiresAtMetadata() {
            config.setBlobTtlDays(7);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            ArgumentCaptor<BlobParallelUploadOptions> captor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            store.storePayload("blob", "payload");

            verify(mockBlobClient).uploadWithResponse(captor.capture(), isNull(), isNull());
            Map<String, String> metadata = captor.getValue().getMetadata();
            assertTrue(metadata.containsKey("expiresAt"));
        }

        @Test
        @DisplayName("storePayload does not add expiresAt when TTL is 0")
        void testStorePayload_noTtl_noExpiresAtMetadata() {
            config.setBlobTtlDays(0);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            ArgumentCaptor<BlobParallelUploadOptions> captor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

            store.storePayload("blob", "payload");

            verify(mockBlobClient).uploadWithResponse(captor.capture(), isNull(), isNull());
            Map<String, String> metadata = captor.getValue().getMetadata();
            assertFalse(metadata.containsKey("expiresAt"));
        }

        @Test
        @DisplayName("cleanupExpiredBlobs skips when TTL not configured")
        void testCleanup_noTtl_skips() {
            config.setBlobTtlDays(0);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            int deleted = store.cleanupExpiredBlobs();

            assertEquals(0, deleted);
            verify(mockContainerClient, never()).listBlobs();
        }

        @SuppressWarnings("unchecked")
        @Test
        @DisplayName("cleanupExpiredBlobs handles per-blob exception gracefully")
        void testCleanup_perBlobException_continuesProcessing() {
            config.setBlobTtlDays(7);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            BlobItem item1 = mock(BlobItem.class);
            when(item1.getName()).thenReturn("blob1");
            BlobItem item2 = mock(BlobItem.class);
            when(item2.getName()).thenReturn("blob2");

            PagedIterable<BlobItem> mockIterable = mock(PagedIterable.class);
            doAnswer(invocation -> {
                Consumer<BlobItem> consumer = invocation.getArgument(0);
                consumer.accept(item1);
                consumer.accept(item2);
                return null;
            }).when(mockIterable).forEach(any());
            when(mockContainerClient.listBlobs()).thenReturn(mockIterable);

            // blob1 throws when getting properties
            BlobClient blobClient1 = mock(BlobClient.class);
            when(mockContainerClient.getBlobClient("blob1")).thenReturn(blobClient1);
            when(blobClient1.getProperties()).thenThrow(new RuntimeException("Storage error"));

            // blob2 returns expired metadata
            BlobClient blobClient2 = mock(BlobClient.class);
            when(mockContainerClient.getBlobClient("blob2")).thenReturn(blobClient2);
            BlobProperties props2 = mock(BlobProperties.class);
            Map<String, String> metadata2 = new HashMap<>();
            metadata2.put("expiresAt", OffsetDateTime.now().minusDays(1).toString());
            when(props2.getMetadata()).thenReturn(metadata2);
            when(blobClient2.getProperties()).thenReturn(props2);

            int deleted = store.cleanupExpiredBlobs();

            // blob1 failed but blob2 was expired and deleted
            assertEquals(1, deleted);
            verify(blobClient2).delete();
        }

        @SuppressWarnings("unchecked")
        @Test
        @DisplayName("cleanupExpiredBlobs skips non-expired blobs")
        void testCleanup_nonExpiredBlob_skipped() {
            config.setBlobTtlDays(7);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            BlobItem item = mock(BlobItem.class);
            when(item.getName()).thenReturn("fresh-blob");

            PagedIterable<BlobItem> mockIterable = mock(PagedIterable.class);
            doAnswer(invocation -> {
                Consumer<BlobItem> consumer = invocation.getArgument(0);
                consumer.accept(item);
                return null;
            }).when(mockIterable).forEach(any());
            when(mockContainerClient.listBlobs()).thenReturn(mockIterable);

            BlobProperties props = mock(BlobProperties.class);
            Map<String, String> metadata = new HashMap<>();
            metadata.put("expiresAt", OffsetDateTime.now().plusDays(5).toString());
            when(props.getMetadata()).thenReturn(metadata);
            when(mockBlobClient.getProperties()).thenReturn(props);

            int deleted = store.cleanupExpiredBlobs();

            assertEquals(0, deleted);
            verify(mockBlobClient, never()).delete();
        }

        @SuppressWarnings("unchecked")
        @Test
        @DisplayName("cleanupExpiredBlobs skips blobs without expiresAt metadata")
        void testCleanup_noExpiresAtMetadata_skipped() {
            config.setBlobTtlDays(7);
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);

            BlobItem item = mock(BlobItem.class);
            when(item.getName()).thenReturn("no-ttl-blob");

            PagedIterable<BlobItem> mockIterable = mock(PagedIterable.class);
            doAnswer(invocation -> {
                Consumer<BlobItem> consumer = invocation.getArgument(0);
                consumer.accept(item);
                return null;
            }).when(mockIterable).forEach(any());
            when(mockContainerClient.listBlobs()).thenReturn(mockIterable);

            BlobProperties props = mock(BlobProperties.class);
            Map<String, String> metadata = new HashMap<>();
            when(props.getMetadata()).thenReturn(metadata);
            when(mockBlobClient.getProperties()).thenReturn(props);

            int deleted = store.cleanupExpiredBlobs();

            assertEquals(0, deleted);
            verify(mockBlobClient, never()).delete();
        }
    }

    // ========== Content Type Retrieval Tests ==========

    @Nested
    @DisplayName("Content Type Retrieval")
    class ContentTypeRetrievalTests {

        @Test
        @DisplayName("getContentType returns HTTP header content type when available")
        void testGetContentType_fromHttpHeader() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "blob");

            BlobProperties props = mock(BlobProperties.class);
            when(props.getContentType()).thenReturn("application/json");
            when(mockBlobClient.getProperties()).thenReturn(props);

            assertEquals("application/json", store.getContentType(pointer));
        }

        @Test
        @DisplayName("getContentType falls back to metadata when HTTP header is empty")
        void testGetContentType_fallbackToMetadata() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "blob");

            BlobProperties props = mock(BlobProperties.class);
            when(props.getContentType()).thenReturn("");
            Map<String, String> metadata = Map.of("contentType", "text/xml");
            when(props.getMetadata()).thenReturn(metadata);
            when(mockBlobClient.getProperties()).thenReturn(props);

            assertEquals("text/xml", store.getContentType(pointer));
        }

        @Test
        @DisplayName("getContentType returns null when HTTP header is null and no metadata")
        void testGetContentType_noContentType() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "blob");

            BlobProperties props = mock(BlobProperties.class);
            when(props.getContentType()).thenReturn(null);
            when(props.getMetadata()).thenReturn(null);
            when(mockBlobClient.getProperties()).thenReturn(props);

            assertNull(store.getContentType(pointer));
        }

        @Test
        @DisplayName("getContentType returns null on exception")
        void testGetContentType_exception_returnsNull() {
            BlobPayloadStore store = new BlobPayloadStore(mockBlobServiceClient, "test-container", config);
            BlobPointer pointer = new BlobPointer("test-container", "blob");

            when(mockBlobClient.getProperties()).thenThrow(new RuntimeException("Storage error"));

            assertNull(store.getContentType(pointer));
        }
    }
}
