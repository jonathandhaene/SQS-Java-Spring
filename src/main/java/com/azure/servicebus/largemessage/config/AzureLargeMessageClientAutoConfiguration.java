package com.azure.servicebus.largemessage.config;

import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.store.BlobPayloadStore;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration for Azure Service Bus Large Message Client.
 * Automatically creates and configures beans when appropriate properties are set.
 */
@Configuration
@EnableConfigurationProperties(LargeMessageClientConfiguration.class)
public class AzureLargeMessageClientAutoConfiguration {

    @Value("${azure.storage.connection-string:}")
    private String storageConnectionString;

    @Value("${azure.storage.container-name:large-messages}")
    private String containerName;

    @Value("${azure.servicebus.connection-string:}")
    private String serviceBusConnectionString;

    @Value("${azure.servicebus.queue-name:my-queue}")
    private String queueName;

    /**
     * Creates a BlobServiceClient bean.
     * Only created when not in receive-only mode.
     *
     * @return the BlobServiceClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "azure.servicebus.large-message-client.receive-only-mode", havingValue = "false", matchIfMissing = true)
    public BlobServiceClient blobServiceClient() {
        if (storageConnectionString == null || storageConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Storage connection string is required. " +
                "Please set azure.storage.connection-string property or AZURE_STORAGE_CONNECTION_STRING environment variable."
            );
        }
        return new BlobServiceClientBuilder()
                .connectionString(storageConnectionString)
                .buildClient();
    }

    /**
     * Creates a BlobPayloadStore bean.
     * Only created when not in receive-only mode.
     *
     * @param blobServiceClient the blob service client
     * @return the BlobPayloadStore instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "azure.servicebus.large-message-client.receive-only-mode", havingValue = "false", matchIfMissing = true)
    public BlobPayloadStore blobPayloadStore(BlobServiceClient blobServiceClient, LargeMessageClientConfiguration config) {
        return new BlobPayloadStore(blobServiceClient, containerName, config);
    }

    /**
     * Creates an AzureServiceBusLargeMessageClient bean.
     * Works in both regular mode (with BlobPayloadStore) and receive-only mode (without).
     *
     * @param config       the large message client configuration
     * @return the AzureServiceBusLargeMessageClient instance
     */
    @Bean
    @ConditionalOnMissingBean
    public AzureServiceBusLargeMessageClient azureServiceBusLargeMessageClient(
            LargeMessageClientConfiguration config,
            org.springframework.beans.factory.ObjectProvider<BlobPayloadStore> payloadStoreProvider) {
        
        if (serviceBusConnectionString == null || serviceBusConnectionString.isEmpty()) {
            throw new IllegalStateException(
                "Azure Service Bus connection string is required. " +
                "Please set azure.servicebus.connection-string property or AZURE_SERVICEBUS_CONNECTION_STRING environment variable."
            );
        }

        // Get BlobPayloadStore if available (will be null in receive-only mode)
        BlobPayloadStore payloadStore = payloadStoreProvider.getIfAvailable();

        return new AzureServiceBusLargeMessageClient(
                serviceBusConnectionString,
                queueName,
                payloadStore,
                config
        );
    }
}
