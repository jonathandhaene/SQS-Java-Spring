package com.azure.servicebus.largemessage.store;

import com.azure.messaging.servicebus.ServiceBusMessage;

/**
 * Functional interface for customizing blob naming strategies.
 * Allows users to define their own blob naming logic (e.g., {tenantId}/{messageId}).
 */
@FunctionalInterface
public interface BlobNameResolver {
    /**
     * Resolves the blob name for a given Service Bus message.
     *
     * @param message the Service Bus message
     * @return the blob name to use for storing the payload
     */
    String resolve(ServiceBusMessage message);
}
