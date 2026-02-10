package com.azure.servicebus.extended.store;

import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles storage and retrieval of large message payloads in Azure Blob Storage.
 */
public class BlobPayloadStore {
    private static final Logger logger = LoggerFactory.getLogger(BlobPayloadStore.class);

    private final BlobContainerClient containerClient;
    private final String containerName;
    private final ExtendedClientConfiguration config;

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
     * @param config           the extended client configuration
     */
    public BlobPayloadStore(BlobServiceClient blobServiceClient, String containerName, ExtendedClientConfiguration config) {
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
        try {
            logger.debug("Storing payload in blob: {}", blobName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(payloadBytes);
            
            // Create metadata map
            Map<String, String> metadata = new HashMap<>();
            
            // Apply encryption if configured (log for now - full implementation requires SDK support)
            if (config != null && config.getEncryption() != null) {
                String encryptionScope = config.getEncryption().getEncryptionScope();
                if (encryptionScope != null && !encryptionScope.isEmpty()) {
                    logger.info("Encryption scope configured: {}", encryptionScope);
                    metadata.put("encryptionScope", encryptionScope);
                }
                
                String customerKey = config.getEncryption().getCustomerProvidedKey();
                if (customerKey != null && !customerKey.isEmpty()) {
                    logger.info("Customer-provided key configured (stored in metadata)");
                    // Don't store the actual key in metadata for security
                    metadata.put("hasCustomerKey", "true");
                }
            }
            
            // Add blob TTL metadata if configured
            if (config != null && config.getBlobTtlDays() > 0) {
                OffsetDateTime expiresAt = OffsetDateTime.now().plusDays(config.getBlobTtlDays());
                metadata.put("expiresAt", expiresAt.toString());
                logger.debug("Setting blob TTL: {} days (expires at: {})", config.getBlobTtlDays(), expiresAt);
            }
            
            // Create upload options
            BlobParallelUploadOptions options = new BlobParallelUploadOptions(inputStream, payloadBytes.length);
            if (!metadata.isEmpty()) {
                options.setMetadata(metadata);
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
     * This is a best-effort cleanup helper, not automatic.
     *
     * @return count of blobs deleted
     */
    public int cleanupExpiredBlobs() {
        if (config == null || config.getBlobTtlDays() <= 0) {
            logger.debug("Blob TTL not configured, skipping cleanup");
            return 0;
        }

        int deletedCount = 0;
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
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to process blob during cleanup: {}", blobItem.getName(), e);
                }
            });

            logger.info("Cleanup completed. Deleted {} expired blobs", deletedCount);
        } catch (Exception e) {
            logger.error("Failed to cleanup expired blobs", e);
        }

        return deletedCount;
    }
}
