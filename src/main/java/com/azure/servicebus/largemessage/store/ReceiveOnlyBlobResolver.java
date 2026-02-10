package com.azure.servicebus.largemessage.store;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Resolver for downloading blob payloads using only SAS URIs.
 * Enables receive-only mode where no storage account credentials are needed.
 */
public class ReceiveOnlyBlobResolver {
    private static final Logger logger = LoggerFactory.getLogger(ReceiveOnlyBlobResolver.class);

    /**
     * Downloads blob content using a SAS URI.
     * This allows receiving messages without needing storage account credentials.
     *
     * @param sasUri the SAS URI containing the blob URL and access token
     * @return the blob content as a string
     * @throws RuntimeException if download fails or SAS token is expired/invalid
     */
    public String getPayloadBySasUri(String sasUri) {
        try {
            logger.debug("Downloading payload using SAS URI");
            
            // Create a BlobClient using just the SAS URI (no credentials needed)
            BlobClient blobClient = new BlobClientBuilder()
                .endpoint(sasUri)
                .buildClient();
            
            // Download the blob content
            byte[] content = blobClient.downloadContent().toBytes();
            String payload = new String(content, StandardCharsets.UTF_8);
            
            logger.debug("Successfully downloaded payload using SAS URI");
            return payload;
        } catch (Exception e) {
            logger.error("Failed to download payload using SAS URI. Possible causes: expired token, invalid URI, or network error", e);
            throw new RuntimeException("Failed to download payload using SAS URI", e);
        }
    }
}
