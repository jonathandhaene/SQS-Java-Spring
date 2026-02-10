package com.azure.servicebus.largemessage.store;

import com.azure.messaging.servicebus.ServiceBusMessage;

import java.util.UUID;

/**
 * Default implementation of BlobNameResolver that generates UUID-based blob names.
 * Uses the configured blob key prefix followed by a random UUID.
 */
public class DefaultBlobNameResolver implements BlobNameResolver {
    private final String blobKeyPrefix;

    /**
     * Creates a new DefaultBlobNameResolver with the specified prefix.
     *
     * @param blobKeyPrefix the prefix to prepend to generated blob names
     */
    public DefaultBlobNameResolver(String blobKeyPrefix) {
        this.blobKeyPrefix = blobKeyPrefix != null ? blobKeyPrefix : "";
    }

    /**
     * Resolves the blob name by generating a UUID-based name.
     *
     * @param message the Service Bus message (not used in default implementation)
     * @return a blob name composed of the prefix and a random UUID
     */
    @Override
    public String resolve(ServiceBusMessage message) {
        return blobKeyPrefix + UUID.randomUUID().toString();
    }
}
