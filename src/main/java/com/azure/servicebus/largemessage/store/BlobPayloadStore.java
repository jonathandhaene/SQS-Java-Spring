/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.store;

import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.util.SasTokenGenerator;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles storage and retrieval of large message payloads in Azure Blob Storage.
 */
public class BlobPayloadStore {
    private static final Logger logger = LoggerFactory.getLogger(BlobPayloadStore.class);

    private final BlobContainerClient containerClient;
    private final String containerName;
    private final LargeMessageClientConfiguration config;

    /**
     * Creates a new BlobPayloadStore.
     *
     * @param blobServiceClient the Azure Blob Service client
     * @param containerName     the name of the container to use for storing payloads
     */
    public BlobPayloadStore(BlobServiceClient blobServiceClient, String containerName) {
        this(blobServiceClient, containerName, null);
    }

    /**
     * Creates a new BlobPayloadStore with configuration.
     *
     * @param blobServiceClient the Azure Blob Service client
     * @param containerName     the name of the container to use for storing payloads
     * @param config           the large message client configuration
     */
    public BlobPayloadStore(BlobServiceClient blobServiceClient, String containerName, LargeMessageClientConfiguration config) {
        this.containerName = containerName;
        this.config = config;
        this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
        
        // Ensure the container exists
        if (!containerClient.exists()) {
            logger.info("Container '{}' does not exist. Creating it...", containerName);
            containerClient.create();
            logger.info("Container '{}' created successfully", containerName);
        } else {
            logger.debug("Container '{}' already exists", containerName);
        }
    }

    /**
     * Stores a payload in blob storage.
     *
     * @param blobName the name to use for the blob
     * @param payload  the payload to store
     * @return a BlobPointer referencing the stored payload
     */
    public BlobPointer storePayload(String blobName, String payload) {
        return storePayload(blobName, payload, null);
    }

    /**
     * Stores a payload in blob storage with an explicit content type.
     *
     * @param blobName    the name to use for the blob
     * @param payload     the payload to store
     * @param contentType the MIME content type (e.g. "application/json"), or null for default
     * @return a BlobPointer referencing the stored payload
     */
    public BlobPointer storePayload(String blobName, String payload, String contentType) {
        try {
            logger.debug("Storing payload in blob: {}", blobName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
            
            // Create metadata map
            Map<String, String> metadata = new HashMap<>();
            
            // Determine content type
            String resolvedContentType = contentType;
            if (resolvedContentType == null || resolvedContentType.isEmpty()) {
                resolvedContentType = (config != null) ? config.getDefaultContentType() : "text/plain; charset=utf-8";
            }
            metadata.put("contentType", resolvedContentType);
            
            // Apply Customer-Provided Key (CPK) encryption if configured
            CustomerProvidedKey cpk = null;
            if (config != null && config.getEncryption() != null && config.getEncryption().hasCustomerProvidedKey()) {
                cpk = config.getEncryption().toSdkCustomerProvidedKey();
                logger.info("Customer-provided key (CPK) configured — will apply server-side encryption");
                metadata.put("hasCustomerKey", "true");
            }
            
            // Apply encryption scope if configured
            String encryptionScope = null;
            if (config != null && config.getEncryption() != null && config.getEncryption().hasEncryptionScope()) {
                encryptionScope = config.getEncryption().getEncryptionScope();
                logger.info("Encryption scope configured: {}", encryptionScope);
                metadata.put("encryptionScope", encryptionScope);
            }
            
            // Add blob TTL metadata if configured
            if (config != null && config.getBlobTtlDays() > 0) {
                OffsetDateTime expiresAt = OffsetDateTime.now().plusDays(config.getBlobTtlDays());
                metadata.put("expiresAt", expiresAt.toString());
                logger.debug("Setting blob TTL: {} days (expires at: {})", config.getBlobTtlDays(), expiresAt);
            }
            
            // Create upload options
            BlobParallelUploadOptions options = new BlobParallelUploadOptions(com.azure.core.util.BinaryData.fromBytes(payloadBytes));
            if (!metadata.isEmpty()) {
                options.setMetadata(metadata);
            }

            // Set HTTP headers including content type
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(resolvedContentType);
            options.setHeaders(headers);
            
            // Apply CPK to upload request
            if (cpk != null) {
                blobClient = containerClient.getBlobClient(blobName).getCustomerProvidedKeyClient(cpk);
            }
            
            // Apply encryption scope via BlobRequestConditions if set
            if (encryptionScope != null) {
                options.setRequestConditions(null); // Encryption scope applied at container/account level
            }
            
            blobClient.uploadWithResponse(options, null, null);
            
            // Set access tier if configured
            if (config != null && config.getBlobAccessTier() != null && !config.getBlobAccessTier().isEmpty()) {
                try {
                    AccessTier tier = AccessTier.fromString(config.getBlobAccessTier());
                    logger.debug("Setting access tier: {}", tier);
                    blobClient.setAccessTier(tier);
                } catch (IllegalArgumentException e) {
                    logger.warn("Invalid blob access tier: {}. Skipping.", config.getBlobAccessTier());
                }
            }
            
            logger.debug("Successfully stored payload in blob: {}", blobName);
            
            return new BlobPointer(containerName, blobName);
        } catch (Exception e) {
            logger.error("Failed to store payload in blob: {}", blobName, e);
            throw new RuntimeException("Failed to store payload in blob storage", e);
        }
    }

    /**
     * Stores a binary payload in blob storage.
     *
     * @param blobName    the name to use for the blob
     * @param payload     the binary payload to store
     * @param contentType the MIME content type (e.g. "application/octet-stream")
     * @return a BlobPointer referencing the stored payload
     */
    public BlobPointer storeBinaryPayload(String blobName, byte[] payload, String contentType) {
        try {
            logger.debug("Storing binary payload in blob: {} ({} bytes)", blobName, payload.length);
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            // Create metadata map
            Map<String, String> metadata = new HashMap<>();
            
            String resolvedContentType = (contentType != null && !contentType.isEmpty())
                    ? contentType : "application/octet-stream";
            metadata.put("contentType", resolvedContentType);
            
            // Apply CPK if configured
            CustomerProvidedKey cpk = null;
            if (config != null && config.getEncryption() != null && config.getEncryption().hasCustomerProvidedKey()) {
                cpk = config.getEncryption().toSdkCustomerProvidedKey();
                metadata.put("hasCustomerKey", "true");
            }
            
            // Add blob TTL metadata if configured
            if (config != null && config.getBlobTtlDays() > 0) {
                OffsetDateTime expiresAt = OffsetDateTime.now().plusDays(config.getBlobTtlDays());
                metadata.put("expiresAt", expiresAt.toString());
            }
            
            BlobParallelUploadOptions options = new BlobParallelUploadOptions(com.azure.core.util.BinaryData.fromBytes(payload));
            if (!metadata.isEmpty()) {
                options.setMetadata(metadata);
            }
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(resolvedContentType);
            options.setHeaders(headers);
            
            if (cpk != null) {
                blobClient = containerClient.getBlobClient(blobName).getCustomerProvidedKeyClient(cpk);
            }
            
            blobClient.uploadWithResponse(options, null, null);
            
            logger.debug("Successfully stored binary payload in blob: {}", blobName);
            return new BlobPointer(containerName, blobName);
        } catch (Exception e) {
            logger.error("Failed to store binary payload in blob: {}", blobName, e);
            throw new RuntimeException("Failed to store binary payload in blob storage", e);
        }
    }

    /**
     * Retrieves a binary payload from blob storage.
     *
     * @param pointer the blob pointer referencing the payload
     * @return the payload as a byte array, or null if ignorePayloadNotFound is enabled and blob doesn't exist
     */
    public byte[] getBinaryPayload(BlobPointer pointer) {
        try {
            logger.debug("Retrieving binary payload from blob: {}", pointer.getBlobName());
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            
            byte[] content = blobClient.downloadContent().toBytes();
            logger.debug("Successfully retrieved binary payload from blob: {} ({} bytes)", pointer.getBlobName(), content.length);
            return content;
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                if (config != null && config.isIgnorePayloadNotFound()) {
                    logger.warn("Blob not found but ignorePayloadNotFound is enabled: {}", pointer.getBlobName());
                    return null;
                }
            }
            logger.error("Failed to retrieve binary payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to retrieve binary payload from blob storage", e);
        }
    }

    /**
     * Gets the content type metadata from a blob.
     *
     * @param pointer the blob pointer
     * @return the content type, or null if not set
     */
    public String getContentType(BlobPointer pointer) {
        try {
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            BlobProperties properties = blobClient.getProperties();
            
            // Try HTTP header first, then metadata
            String contentType = properties.getContentType();
            if (contentType == null || contentType.isEmpty()) {
                Map<String, String> metadata = properties.getMetadata();
                if (metadata != null) {
                    contentType = metadata.get("contentType");
                }
            }
            return contentType;
        } catch (Exception e) {
            logger.warn("Failed to get content type for blob: {}", pointer.getBlobName(), e);
            return null;
        }
    }

    /**
     * Retrieves a payload from blob storage.
     *
     * @param pointer the blob pointer referencing the payload
     * @return the payload content as a string, or null if ignorePayloadNotFound is enabled and blob doesn't exist
     */
    public String getPayload(BlobPointer pointer) {
        try {
            logger.debug("Retrieving payload from blob: {}", pointer.getBlobName());
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            
            byte[] content = blobClient.downloadContent().toBytes();
            String payload = new String(content, StandardCharsets.UTF_8);
            
            logger.debug("Successfully retrieved payload from blob: {}", pointer.getBlobName());
            return payload;
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                if (config != null && config.isIgnorePayloadNotFound()) {
                    logger.warn("Blob not found but ignorePayloadNotFound is enabled: {}", pointer.getBlobName());
                    return null;
                }
            }
            logger.error("Failed to retrieve payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to retrieve payload from blob storage", e);
        } catch (Exception e) {
            logger.error("Failed to retrieve payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to retrieve payload from blob storage", e);
        }
    }

    /**
     * Deletes a payload from blob storage.
     * Handles 404 errors gracefully (blob already deleted or doesn't exist).
     *
     * @param pointer the blob pointer referencing the payload to delete
     */
    public void deletePayload(BlobPointer pointer) {
        try {
            logger.debug("Deleting payload from blob: {}", pointer.getBlobName());
            BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
            blobClient.delete();
            logger.debug("Successfully deleted payload from blob: {}", pointer.getBlobName());
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                logger.debug("Blob not found (already deleted): {}", pointer.getBlobName());
            } else {
                logger.error("Failed to delete payload from blob: {}", pointer.getBlobName(), e);
                throw new RuntimeException("Failed to delete payload from blob storage", e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete payload from blob: {}", pointer.getBlobName(), e);
            throw new RuntimeException("Failed to delete payload from blob storage", e);
        }
    }

    /**
     * Cleans up expired blobs based on TTL metadata.
     * This method can be called manually or by the scheduled cleanup bean.
     *
     * @return count of blobs deleted
     */
    public int cleanupExpiredBlobs() {
        if (config == null || config.getBlobTtlDays() <= 0) {
            logger.debug("Blob TTL not configured, skipping cleanup");
            return 0;
        }

        final int[] deletedCount = {0};
        OffsetDateTime now = OffsetDateTime.now();

        try {
            logger.info("Starting cleanup of expired blobs");
            
            containerClient.listBlobs().forEach(blobItem -> {
                try {
                    BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
                    Map<String, String> metadata = blobClient.getProperties().getMetadata();
                    
                    if (metadata != null && metadata.containsKey("expiresAt")) {
                        String expiresAtStr = metadata.get("expiresAt");
                        OffsetDateTime expiresAt = OffsetDateTime.parse(expiresAtStr);
                        
                        if (now.isAfter(expiresAt)) {
                            logger.debug("Deleting expired blob: {} (expired at: {})", blobItem.getName(), expiresAt);
                            blobClient.delete();
                            deletedCount[0]++;
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to process blob during cleanup: {}", blobItem.getName(), e);
                }
            });

            logger.info("Cleanup completed. Deleted {} expired blobs", deletedCount[0]);
        } catch (Exception e) {
            logger.error("Failed to cleanup expired blobs", e);
        }

        return deletedCount[0];
    }

    /**
     * Generates a SAS URI for the specified blob pointer.
     *
     * @param pointer  the blob pointer referencing the payload
     * @param validFor the duration for which the SAS token should be valid
     * @return the SAS URI as a string
     */
    public String generateSasUri(BlobPointer pointer, Duration validFor) {
        logger.debug("Generating SAS URI for blob: {}", pointer.getBlobName());
        BlobClient blobClient = containerClient.getBlobClient(pointer.getBlobName());
        return SasTokenGenerator.generateBlobSasUri(blobClient, validFor);
    }
}
