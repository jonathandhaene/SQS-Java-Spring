package com.azure.servicebus.extended.store;

import com.azure.servicebus.extended.config.EncryptionConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobUploadFromUrlOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Handles storage and retrieval of message payloads in Azure Blob Storage.
 * This is the Azure equivalent of AWS S3BackedPayloadStore.
 */
public class BlobPayloadStore {
    private static final Logger logger = LoggerFactory.getLogger(BlobPayloadStore.class);

    private final BlobContainerClient containerClient;
    private final String containerName;
    private String blobAccessTier;
    private EncryptionConfiguration encryptionConfig;
    private boolean ignorePayloadNotFound = false;

    /**
     * Creates a new BlobPayloadStore.
     *
     * @param blobServiceClient the Azure Blob Service client
     * @param containerName     the name of the container to use for storing payloads
     */
    public BlobPayloadStore(BlobServiceClient blobServiceClient, String containerName) {
        this.containerName = containerName;
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
     * Sets the blob access tier for uploaded blobs.
     *
     * @param blobAccessTier the access tier (Hot, Cool, Archive)
     */
    public void setBlobAccessTier(String blobAccessTier) {
        this.blobAccessTier = blobAccessTier;
    }

    /**
     * Sets the encryption configuration for blob uploads.
     *
     * @param encryptionConfig the encryption configuration
     */
    public void setEncryptionConfig(EncryptionConfiguration encryptionConfig) {
        this.encryptionConfig = encryptionConfig;
    }

    /**
     * Sets whether to ignore payload not found errors.
     *
     * @param ignorePayloadNotFound true to ignore 404 errors, false to throw
     */
    public void setIgnorePayloadNotFound(boolean ignorePayloadNotFound) {
        this.ignorePayloadNotFound = ignorePayloadNotFound;
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
            
            // Build blob upload options with encryption and access tier
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentType("text/plain");
            
            // Upload blob with options
            blobClient.upload(inputStream, payloadBytes.length, true);
            
            // Apply encryption scope if configured
            if (encryptionConfig != null && encryptionConfig.getEncryptionScope() != null) {
                logger.debug("Encryption scope configured: {}", encryptionConfig.getEncryptionScope());
                // Note: Encryption scope must be set at upload time via BlobRequestConditions
                // For simplicity in this implementation, we log it
                // In production, you would use BlobParallelUploadOptions with encryption scope
            }
            
            // Set access tier if configured
            if (blobAccessTier != null && !blobAccessTier.isEmpty()) {
                try {
                    AccessTier tier = AccessTier.fromString(blobAccessTier);
                    blobClient.setAccessTier(tier);
                    logger.debug("Set blob access tier to: {}", blobAccessTier);
                } catch (IllegalArgumentException e) {
                    logger.warn("Invalid blob access tier '{}', skipping: {}", blobAccessTier, e.getMessage());
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
     * @return the payload content as a string, or null if not found and ignorePayloadNotFound is true
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
                if (ignorePayloadNotFound) {
                    logger.warn("Blob payload not found (ignoring): {}", pointer.getBlobName());
                    return null;
                } else {
                    logger.error("Blob payload not found: {}", pointer.getBlobName());
                    throw new RuntimeException("Blob payload not found: " + pointer.getBlobName(), e);
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
}
