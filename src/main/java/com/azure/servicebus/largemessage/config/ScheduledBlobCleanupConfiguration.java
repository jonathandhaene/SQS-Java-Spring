/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Scheduled cleanup configuration for expired blob payloads.
 *
 * <p>When {@code azure.servicebus.large-message-client.ttl-cleanup-interval-minutes} is set to a
 * positive value, this bean runs a periodic job that removes blobs whose TTL metadata has expired.</p>
 *
 * <p>As an alternative, consider using
 * <a href="https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview">
 * Azure Blob Storage lifecycle management policies</a> for production workloads.</p>
 *
 * <p><b>Disclaimer:</b> This is for illustration purposes only and is NOT production-ready.</p>
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "azure.servicebus.large-message-client.ttl-cleanup-interval-minutes", matchIfMissing = false)
public class ScheduledBlobCleanupConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledBlobCleanupConfiguration.class);

    private final BlobPayloadStore payloadStore;
    private final LargeMessageClientConfiguration config;

    public ScheduledBlobCleanupConfiguration(BlobPayloadStore payloadStore, LargeMessageClientConfiguration config) {
        this.payloadStore = payloadStore;
        this.config = config;
        logger.info("Scheduled blob TTL cleanup enabled (interval: {} minutes)", config.getTtlCleanupIntervalMinutes());
    }

    /**
     * Periodically cleans up expired blobs based on TTL metadata.
     * The interval is configured via {@code azure.servicebus.large-message-client.ttl-cleanup-interval-minutes}.
     * Uses a fixed delay (in milliseconds) derived from the configured minutes.
     */
    @Scheduled(fixedDelayString = "#{${azure.servicebus.large-message-client.ttl-cleanup-interval-minutes:60} * 60 * 1000}")
    public void cleanupExpiredBlobs() {
        if (config.getTtlCleanupIntervalMinutes() <= 0) {
            return;
        }
        try {
            logger.debug("Running scheduled TTL cleanup...");
            int deleted = payloadStore.cleanupExpiredBlobs();
            if (deleted > 0) {
                logger.info("Scheduled TTL cleanup deleted {} expired blobs", deleted);
            }
        } catch (Exception e) {
            logger.error("Scheduled TTL cleanup failed", e);
        }
    }
}
