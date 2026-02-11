/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ScheduledBlobCleanupConfiguration}.
 */
@DisplayName("ScheduledBlobCleanupConfiguration")
class ScheduledBlobCleanupConfigurationTest {

    @Test
    @DisplayName("Cleanup method delegates to payloadStore.cleanupExpiredBlobs()")
    void testCleanupDelegatesToPayloadStore() {
        BlobPayloadStore mockStore = mock(BlobPayloadStore.class);
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
        config.setTtlCleanupIntervalMinutes(30);

        when(mockStore.cleanupExpiredBlobs()).thenReturn(5);

        ScheduledBlobCleanupConfiguration cleanupConfig =
                new ScheduledBlobCleanupConfiguration(mockStore, config);

        cleanupConfig.cleanupExpiredBlobs();

        verify(mockStore).cleanupExpiredBlobs();
    }

    @Test
    @DisplayName("Cleanup skips when interval is 0")
    void testCleanupSkipsWhenIntervalIsZero() {
        BlobPayloadStore mockStore = mock(BlobPayloadStore.class);
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
        config.setTtlCleanupIntervalMinutes(0);

        ScheduledBlobCleanupConfiguration cleanupConfig =
                new ScheduledBlobCleanupConfiguration(mockStore, config);

        cleanupConfig.cleanupExpiredBlobs();

        verify(mockStore, never()).cleanupExpiredBlobs();
    }

    @Test
    @DisplayName("Cleanup handles exception gracefully")
    void testCleanupHandlesException() {
        BlobPayloadStore mockStore = mock(BlobPayloadStore.class);
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
        config.setTtlCleanupIntervalMinutes(15);

        when(mockStore.cleanupExpiredBlobs()).thenThrow(new RuntimeException("Storage error"));

        ScheduledBlobCleanupConfiguration cleanupConfig =
                new ScheduledBlobCleanupConfiguration(mockStore, config);

        // Should not throw
        assertDoesNotThrow(cleanupConfig::cleanupExpiredBlobs);
    }

    @Test
    @DisplayName("Constructor sets up configuration correctly")
    void testConstructor() {
        BlobPayloadStore mockStore = mock(BlobPayloadStore.class);
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();
        config.setTtlCleanupIntervalMinutes(60);

        ScheduledBlobCleanupConfiguration cleanupConfig =
                new ScheduledBlobCleanupConfiguration(mockStore, config);

        // Verify it was constructed without error and can run cleanup
        assertDoesNotThrow(cleanupConfig::cleanupExpiredBlobs);
    }
}
