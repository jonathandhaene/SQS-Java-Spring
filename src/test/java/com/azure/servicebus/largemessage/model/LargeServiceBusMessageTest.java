/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LargeServiceBusMessageTest {

    @Test
    void testSimpleConstructor() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", "value1");
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-123", "Hello World", properties, true, pointer);

        // Assert
        assertEquals("msg-123", message.getMessageId());
        assertEquals("Hello World", message.getBody());
        assertEquals(properties, message.getApplicationProperties());
        assertTrue(message.isPayloadFromBlob());
        assertEquals(pointer, message.getBlobPointer());
        assertNull(message.getDeadLetterReason());
        assertNull(message.getDeadLetterDescription());
        assertEquals(0, message.getDeliveryCount());
    }

    @Test
    void testFullConstructor() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        BlobPointer pointer = new BlobPointer("container", "blob");

        // Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-456", "payload", properties, false, pointer,
                "ProcessingFailure", "Failed to process", 3);

        // Assert
        assertEquals("msg-456", message.getMessageId());
        assertEquals("payload", message.getBody());
        assertFalse(message.isPayloadFromBlob());
        assertEquals(pointer, message.getBlobPointer());
        assertEquals("ProcessingFailure", message.getDeadLetterReason());
        assertEquals("Failed to process", message.getDeadLetterDescription());
        assertEquals(3, message.getDeliveryCount());
    }

    @Test
    void testNotFromBlob() {
        // Arrange & Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-789", "Small message", new HashMap<>(), false, null);

        // Assert
        assertFalse(message.isPayloadFromBlob());
        assertNull(message.getBlobPointer());
    }

    @Test
    void testNullBody() {
        // Arrange & Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-null", null, new HashMap<>(), false, null);

        // Assert
        assertNull(message.getBody());
    }

    @Test
    void testToString_WithBody() {
        // Arrange
        BlobPointer pointer = new BlobPointer("container", "blob");
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-str", "test body", new HashMap<>(), true, pointer,
                "reason", null, 2);

        // Act
        String str = message.toString();

        // Assert
        assertNotNull(str);
        assertTrue(str.contains("msg-str"));
        assertTrue(str.contains("payloadFromBlob=true"));
        assertTrue(str.contains("deliveryCount=2"));
        assertTrue(str.contains("deadLetterReason='reason'"));
        assertTrue(str.contains("bodyLength=9")); // "test body".length() == 9
    }

    @Test
    void testToString_WithNullBody() {
        // Arrange
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-null-body", null, new HashMap<>(), false, null);

        // Act
        String str = message.toString();

        // Assert
        assertNotNull(str);
        assertTrue(str.contains("bodyLength=0"));
    }

    @Test
    void testApplicationProperties() {
        // Arrange
        Map<String, Object> properties = new HashMap<>();
        properties.put("tenantId", "tenant-123");
        properties.put("priority", 5);
        properties.put("isUrgent", true);

        // Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-props", "body", properties, false, null);

        // Assert
        Map<String, Object> returnedProps = message.getApplicationProperties();
        assertEquals(3, returnedProps.size());
        assertEquals("tenant-123", returnedProps.get("tenantId"));
        assertEquals(5, returnedProps.get("priority"));
        assertEquals(true, returnedProps.get("isUrgent"));
    }

    @Test
    void testNullApplicationProperties() {
        // Arrange & Act
        LargeServiceBusMessage message = new LargeServiceBusMessage(
                "msg-null-props", "body", null, false, null);

        // Assert
        assertNull(message.getApplicationProperties());
    }
}
