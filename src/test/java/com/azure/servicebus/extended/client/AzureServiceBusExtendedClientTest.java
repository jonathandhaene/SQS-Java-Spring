package com.azure.servicebus.extended.client;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.servicebus.extended.config.ExtendedClientConfiguration;
import com.azure.servicebus.extended.model.BlobPointer;
import com.azure.servicebus.extended.model.ExtendedServiceBusMessage;
import com.azure.servicebus.extended.store.BlobPayloadStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AzureServiceBusExtendedClient.
 */
class AzureServiceBusExtendedClientTest {

    private ServiceBusSenderClient mockSenderClient;
    private ServiceBusReceiverClient mockReceiverClient;
    private BlobPayloadStore mockPayloadStore;
    private ExtendedClientConfiguration config;
    private AzureServiceBusExtendedClient client;

    @BeforeEach
    void setUp() {
        mockSenderClient = mock(ServiceBusSenderClient.class);
        mockReceiverClient = mock(ServiceBusReceiverClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
        
        config = new ExtendedClientConfiguration();
        config.setMessageSizeThreshold(1024); // 1 KB for testing
        config.setAlwaysThroughBlob(false);
        config.setCleanupBlobOnDelete(true);
        config.setBlobKeyPrefix("");

        client = new AzureServiceBusExtendedClient(
                mockSenderClient,
                mockReceiverClient,
                mockPayloadStore,
                config
        );
    }

    @Test
    void testSmallMessage_sentDirectly() {
        // Arrange
        String smallMessage = "Small test message";
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals(smallMessage, capturedMessage.getBody().toString());
        assertFalse(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
        
        // Verify no blob interaction
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testLargeMessage_offloadedToBlob() {
        // Arrange
        String largeMessage = generateMessage(2048); // 2 KB, exceeds threshold
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(largeMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        String messageBody = capturedMessage.getBody().toString();
        
        // Verify message body contains blob pointer JSON
        assertTrue(messageBody.contains("containerName"));
        assertTrue(messageBody.contains("blobName"));
        
        // Verify application properties
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
        // By default, uses legacy attribute name
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME));
    }

    @Test
    void testAlwaysThroughBlob_smallMessageStillOffloaded() {
        // Arrange
        config.setAlwaysThroughBlob(true);
        String smallMessage = "Small message";
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(smallMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(smallMessage);

        // Assert
        verify(mockPayloadStore, times(1)).storePayload(anyString(), eq(smallMessage));
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        assertEquals("true", capturedMessage.getApplicationProperties().get(
                ExtendedClientConfiguration.BLOB_POINTER_MARKER));
    }

    @Test
    void testDeletePayload_cleansUpBlob() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true, // from blob
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, times(1)).deletePayload(pointer);
    }

    @Test
    void testDeletePayload_skipsWhenNotFromBlob() {
        // Arrange
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                false, // not from blob
                null
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testDeletePayload_skipsWhenCleanupDisabled() {
        // Arrange
        config.setCleanupBlobOnDelete(false);
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");
        ExtendedServiceBusMessage message = new ExtendedServiceBusMessage(
                "msg-id",
                "body",
                new HashMap<>(),
                true,
                pointer
        );

        // Act
        client.deletePayload(message);

        // Assert
        verify(mockPayloadStore, never()).deletePayload(any());
    }

    @Test
    void testSendMessage_withApplicationProperties() {
        // Arrange
        String messageBody = "Test message";
        Map<String, Object> customProps = new HashMap<>();
        customProps.put("customKey", "customValue");
        customProps.put("timestamp", 12345L);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(messageBody, customProps);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        assertEquals("customValue", capturedMessage.getApplicationProperties().get("customKey"));
        assertEquals(12345L, capturedMessage.getApplicationProperties().get("timestamp"));
    }

    @Test
    void testBlobPointer_serializationRoundTrip() {
        // Arrange
        BlobPointer original = new BlobPointer("my-container", "my-blob-123");

        // Act
        String json = original.toJson();
        BlobPointer deserialized = BlobPointer.fromJson(json);

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("my-container"));
        assertTrue(json.contains("my-blob-123"));
        assertEquals(original, deserialized);
        assertEquals(original.getContainerName(), deserialized.getContainerName());
        assertEquals(original.getBlobName(), deserialized.getBlobName());
    }

    @Test
    void testBlobPointer_equality() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob");
        BlobPointer pointer2 = new BlobPointer("container", "blob");
        BlobPointer pointer3 = new BlobPointer("container", "different-blob");

        // Assert
        assertEquals(pointer1, pointer2);
        assertNotEquals(pointer1, pointer3);
        assertEquals(pointer1.hashCode(), pointer2.hashCode());
    }

    @Test
    void testBlobPointer_toString() {
        // Arrange
        BlobPointer pointer = new BlobPointer("test-container", "test-blob");

        // Act
        String string = pointer.toString();

        // Assert
        assertTrue(string.contains("test-container"));
        assertTrue(string.contains("test-blob"));
    }

    // ========== Tests for New Features ==========

    @Test
    void testPayloadSupportDisabled_messagesSentDirectly() {
        // Arrange
        config.setPayloadSupportEnabled(false);
        String largeMessage = generateMessage(5000); // Large message
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        // Message should be sent directly, not offloaded
        assertEquals(largeMessage, capturedMessage.getBody().toString());
        verify(mockPayloadStore, never()).storePayload(anyString(), anyString());
    }

    @Test
    void testUserAgentTracking_addedOnSend() {
        // Arrange
        String messageBody = "Test message";
        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(messageBody);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        // User-agent should be added
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT));
        String userAgent = (String) capturedMessage.getApplicationProperties().get(
                ExtendedClientConfiguration.EXTENDED_CLIENT_USER_AGENT);
        assertTrue(userAgent.contains("AzureServiceBusExtendedClient"));
    }

    @Test
    void testLegacyAttributeName_usedWhenConfigured() {
        // Arrange
        config.setUseLegacyReservedAttributeName(true);
        String largeMessage = generateMessage(2048);
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        // Should use legacy attribute name
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME));
        assertFalse(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME));
    }

    @Test
    void testModernAttributeName_usedWhenLegacyDisabled() {
        // Arrange
        config.setUseLegacyReservedAttributeName(false);
        String largeMessage = generateMessage(2048);
        BlobPointer expectedPointer = new BlobPointer("test-container", "test-blob");
        
        when(mockPayloadStore.storePayload(anyString(), eq(largeMessage)))
                .thenReturn(expectedPointer);

        ArgumentCaptor<ServiceBusMessage> messageCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        // Act
        client.sendMessage(largeMessage);

        // Assert
        verify(mockSenderClient, times(1)).sendMessage(messageCaptor.capture());
        ServiceBusMessage capturedMessage = messageCaptor.getValue();
        
        // Should use modern attribute name
        assertTrue(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME));
        assertFalse(capturedMessage.getApplicationProperties().containsKey(
                ExtendedClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME));
    }

    @Test
    void testPropertyValidation_tooManyProperties() {
        // Arrange
        config.setMaxAllowedProperties(3);
        String messageBody = "Test message";
        Map<String, Object> tooManyProps = new HashMap<>();
        tooManyProps.put("key1", "value1");
        tooManyProps.put("key2", "value2");
        tooManyProps.put("key3", "value3");
        tooManyProps.put("key4", "value4"); // Exceeds limit

        // Act & Assert
        Exception exception = assertThrows(RuntimeException.class, () -> {
            client.sendMessage(messageBody, tooManyProps);
        });
        assertTrue(exception.getMessage().contains("Failed to send message"));
    }

    @Test
    void testPropertyValidation_reservedNameUsed() {
        // Arrange
        String messageBody = "Test message";
        Map<String, Object> reservedProps = new HashMap<>();
        reservedProps.put(ExtendedClientConfiguration.RESERVED_ATTRIBUTE_NAME, 12345);

        // Act & Assert
        Exception exception = assertThrows(RuntimeException.class, () -> {
            client.sendMessage(messageBody, reservedProps);
        });
        assertTrue(exception.getMessage().contains("Failed to send message"));
    }

    @Test
    void testDeletePayloadBatch_deletesMultiplePayloads() {
        // Arrange
        BlobPointer pointer1 = new BlobPointer("container", "blob1");
        BlobPointer pointer2 = new BlobPointer("container", "blob2");
        
        ExtendedServiceBusMessage msg1 = new ExtendedServiceBusMessage(
                "msg-1", "body1", new HashMap<>(), true, pointer1);
        ExtendedServiceBusMessage msg2 = new ExtendedServiceBusMessage(
                "msg-2", "body2", new HashMap<>(), true, pointer2);
        ExtendedServiceBusMessage msg3 = new ExtendedServiceBusMessage(
                "msg-3", "body3", new HashMap<>(), false, null); // Not from blob
        
        List<ExtendedServiceBusMessage> messages = Arrays.asList(msg1, msg2, msg3);

        // Act
        client.deletePayloadBatch(messages);

        // Assert
        verify(mockPayloadStore, times(1)).deletePayload(pointer1);
        verify(mockPayloadStore, times(1)).deletePayload(pointer2);
        verify(mockPayloadStore, times(2)).deletePayload(any());
    }

    @Test
    void testBlobKeyPrefixValidation_validPrefix() {
        // Should not throw
        config.setBlobKeyPrefix("messages/queue1/");
        config.setBlobKeyPrefix("valid_prefix-123");
        config.setBlobKeyPrefix("");
    }

    @Test
    void testBlobKeyPrefixValidation_invalidCharacters() {
        // Act & Assert
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            config.setBlobKeyPrefix("invalid@prefix");
        });
        assertTrue(exception.getMessage().contains("invalid characters"));
    }

    @Test
    void testBlobKeyPrefixValidation_tooLong() {
        // Arrange
        String longPrefix = "a".repeat(1000); // Exceeds 988 character limit

        // Act & Assert
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            config.setBlobKeyPrefix(longPrefix);
        });
        assertTrue(exception.getMessage().contains("maximum length"));
    }

    /**
     * Helper method to generate a message of specified size.
     */
    private String generateMessage(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
