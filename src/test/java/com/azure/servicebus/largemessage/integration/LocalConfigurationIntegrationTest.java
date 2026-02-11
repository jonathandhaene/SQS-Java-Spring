/*
 * DISCLAIMER: This code is provided for illustration and educational purposes only.
 * It is NOT production-ready and should NOT be used in production without thorough
 * review, testing, and modification. The author(s) accept NO responsibility or
 * liability for any issues arising from the use of this software.
 * USE AT YOUR OWN RISK.
 */

package com.azure.servicebus.largemessage.integration;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import com.azure.servicebus.largemessage.client.AzureServiceBusLargeMessageClient;
import com.azure.servicebus.largemessage.config.DefaultMessageSizeCriteria;
import com.azure.servicebus.largemessage.config.LargeMessageClientConfiguration;
import com.azure.servicebus.largemessage.config.MessageSizeCriteria;
import com.azure.servicebus.largemessage.model.BlobPointer;
import com.azure.servicebus.largemessage.model.LargeServiceBusMessage;
import com.azure.servicebus.largemessage.store.*;
import com.azure.servicebus.largemessage.util.DuplicateDetectionHelper;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests that exercise configuration variants and feature toggles
 * of the large message client.
 *
 * <p>All tests run locally with mocked dependencies — no Azure credentials
 * required. Run via:
 * <pre>mvn verify -Pintegration-test-local</pre>
 */
@DisplayName("Local Configuration & Feature Toggle Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LocalConfigurationIntegrationTest extends IntegrationTestBase {

    private ServiceBusSenderClient mockSender;
    private ServiceBusReceiverClient mockReceiver;
    private BlobPayloadStore mockPayloadStore;

    @BeforeEach
    void setUp() {
        mockSender = mock(ServiceBusSenderClient.class);
        mockReceiver = mock(ServiceBusReceiverClient.class);
        mockPayloadStore = mock(BlobPayloadStore.class);
    }

    // =========================================================================
    // Custom MessageSizeCriteria
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Custom MessageSizeCriteria controls offloading decision")
    void testCustomMessageSizeCriteria() {
        LargeMessageClientConfiguration config = createTestConfiguration();

        // Custom criteria: offload only if the body contains "OFFLOAD_ME"
        MessageSizeCriteria custom = (body, props) -> body.contains("OFFLOAD_ME");
        config.setMessageSizeCriteria(custom);

        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        // Should NOT offload (doesn't contain keyword)
        client.sendMessage("normal small message");
        verifyNoInteractions(mockPayloadStore);

        // Should offload (contains keyword)
        client.sendMessage("OFFLOAD_ME please");
        verify(mockPayloadStore).storePayload(anyString(), eq("OFFLOAD_ME please"));

        client.close();
        logger.info("✓ Custom MessageSizeCriteria respected");
    }

    // =========================================================================
    // Custom BlobNameResolver
    // =========================================================================

    @Test
    @Order(2)
    @DisplayName("Custom BlobNameResolver controls generated blob names")
    void testCustomBlobNameResolver() {
        LargeMessageClientConfiguration config = createTestConfiguration();

        // Custom resolver: use "<sessionId>/<messageId>.json" pattern
        BlobNameResolver customResolver = msg ->
                "custom-prefix/" + System.nanoTime() + ".json";
        config.setBlobNameResolver(customResolver);

        BlobPointer pointer = new BlobPointer("c", "custom-prefix/123.json");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        client.sendMessage(generateLargeMessage());

        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPayloadStore).storePayload(nameCaptor.capture(), anyString());
        assertTrue(nameCaptor.getValue().startsWith("custom-prefix/"),
                "Blob name should use custom resolver");
        assertTrue(nameCaptor.getValue().endsWith(".json"),
                "Blob name should end with .json");

        client.close();
        logger.info("✓ Custom BlobNameResolver generated correct name: {}", nameCaptor.getValue());
    }

    // =========================================================================
    // Custom MessageBodyReplacer
    // =========================================================================

    @Test
    @Order(3)
    @DisplayName("Custom MessageBodyReplacer controls the replacement body")
    void testCustomBodyReplacer() {
        LargeMessageClientConfiguration config = createTestConfiguration();

        // Custom replacer: include a prefix before the pointer JSON
        MessageBodyReplacer customReplacer = (original, pointer) ->
                "BLOB_REF:" + pointer.toJson();
        config.setBodyReplacer(customReplacer);

        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        client.sendMessage(generateLargeMessage());

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertTrue(captor.getValue().getBody().toString().startsWith("BLOB_REF:"),
                "Body should use custom replacer format");

        client.close();
        logger.info("✓ Custom MessageBodyReplacer applied");
    }

    // =========================================================================
    // Legacy attribute name
    // =========================================================================

    @Test
    @Order(4)
    @DisplayName("Legacy reserved attribute name is used when configured")
    void testLegacyAttributeName() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setUseLegacyReservedAttributeName(true);

        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        client.sendMessage(generateLargeMessage());

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertNotNull(captor.getValue().getApplicationProperties()
                        .get(LargeMessageClientConfiguration.LEGACY_RESERVED_ATTRIBUTE_NAME),
                "Legacy attribute name should be used");

        client.close();
        logger.info("✓ Legacy reserved attribute name used");
    }

    // =========================================================================
    // Payload support disabled
    // =========================================================================

    @Test
    @Order(5)
    @DisplayName("Payload support disabled — large messages sent directly")
    void testPayloadSupportDisabled() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setPayloadSupportEnabled(false);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        String largeBody = generateLargeMessage();
        client.sendMessage(largeBody);

        verifyNoInteractions(mockPayloadStore);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        assertEquals(largeBody, captor.getValue().getBody().toString());

        client.close();
        logger.info("✓ Large message sent directly when payload support disabled");
    }

    // =========================================================================
    // Cleanup disabled
    // =========================================================================

    @Test
    @Order(6)
    @DisplayName("Cleanup disabled — deletePayload is a no-op")
    void testCleanupDisabled() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setCleanupBlobOnDelete(false);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        BlobPointer pointer = new BlobPointer("c", "b");
        LargeServiceBusMessage msg = new LargeServiceBusMessage(
                "msg-1", "body", Map.of(), true, pointer);

        client.deletePayload(msg);

        verifyNoInteractions(mockPayloadStore);

        client.close();
        logger.info("✓ Delete skipped when cleanup is disabled");
    }

    // =========================================================================
    // SAS URI generation
    // =========================================================================

    @Test
    @Order(7)
    @DisplayName("SAS URI is added to message properties when enabled")
    void testSasUriGeneration() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setSasEnabled(true);

        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);
        when(mockPayloadStore.generateSasUri(any(BlobPointer.class), any(Duration.class)))
                .thenReturn("https://storage.blob.core.windows.net/c/b?sv=2024&sig=xyz");

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        client.sendMessage(generateLargeMessage());

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());
        String sasUri = (String) captor.getValue().getApplicationProperties()
                .get(config.getMessagePropertyForBlobSasUri());
        assertNotNull(sasUri, "SAS URI should be present in properties");
        assertTrue(sasUri.contains("sig="), "SAS URI should contain a signature");

        client.close();
        logger.info("✓ SAS URI added to message properties");
    }

    // =========================================================================
    // Duplicate detection hash
    // =========================================================================

    @Test
    @Order(8)
    @DisplayName("Duplicate detection hash is deterministic for identical content")
    void testDuplicateDetectionHashIsDeterministic() {
        String content = "identical content";
        String hash1 = DuplicateDetectionHelper.computeContentHash(content);
        String hash2 = DuplicateDetectionHelper.computeContentHash(content);
        assertEquals(hash1, hash2, "Same content should produce same hash");

        String differentHash = DuplicateDetectionHelper.computeContentHash("different content");
        assertNotEquals(hash1, differentHash, "Different content should produce different hash");

        logger.info("✓ Duplicate detection hash is deterministic");
    }

    // =========================================================================
    // Default components initialization
    // =========================================================================

    @Test
    @Order(9)
    @DisplayName("Default components are initialised from configuration")
    void testDefaultComponentsInitialisation() {
        LargeMessageClientConfiguration config = new LargeMessageClientConfiguration();

        assertNotNull(config.getBlobNameResolver(), "BlobNameResolver should be initialised");
        assertNotNull(config.getBodyReplacer(), "BodyReplacer should be initialised");
        assertNotNull(config.getMessageSizeCriteria(), "MessageSizeCriteria should be initialised");

        assertTrue(config.getBlobNameResolver() instanceof DefaultBlobNameResolver);
        assertTrue(config.getBodyReplacer() instanceof DefaultMessageBodyReplacer);
        assertTrue(config.getMessageSizeCriteria() instanceof DefaultMessageSizeCriteria);

        logger.info("✓ Default components initialised correctly");
    }

    // =========================================================================
    // Blob key prefix
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Blob key prefix is prepended to generated names")
    void testBlobKeyPrefix() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setBlobKeyPrefix("my-app/prod/");

        BlobPointer pointer = new BlobPointer("c", "b");
        when(mockPayloadStore.storePayload(anyString(), anyString())).thenReturn(pointer);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);
        client.sendMessage(generateLargeMessage());

        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPayloadStore).storePayload(nameCaptor.capture(), anyString());
        assertTrue(nameCaptor.getValue().startsWith("my-app/prod/"),
                "Blob name should start with configured prefix");

        client.close();
        logger.info("✓ Blob key prefix applied: {}", nameCaptor.getValue());
    }

    // =========================================================================
    // User-agent injection and stripping
    // =========================================================================

    @Test
    @Order(11)
    @DisplayName("User-agent header is injected on send and stripped on receive")
    void testUserAgentInjectionAndStrip() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        config.setPayloadSupportEnabled(false);

        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        // Send — user-agent should be injected
        client.sendMessage(generateSmallMessage());

        ArgumentCaptor<ServiceBusMessage> sendCaptor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(sendCaptor.capture());
        assertEquals(LargeMessageClientConfiguration.USER_AGENT_VALUE,
                sendCaptor.getValue().getApplicationProperties()
                        .get(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT));

        // Receive — user-agent should be stripped
        ServiceBusReceivedMessage mockMsg = mock(ServiceBusReceivedMessage.class);
        when(mockMsg.getMessageId()).thenReturn("m");
        when(mockMsg.getBody()).thenReturn(
                com.azure.core.util.BinaryData.fromString("hello"));
        when(mockMsg.getDeliveryCount()).thenReturn(0L);
        Map<String, Object> appProps = new HashMap<>();
        appProps.put(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT,
                LargeMessageClientConfiguration.USER_AGENT_VALUE);
        when(mockMsg.getApplicationProperties()).thenReturn(appProps);

        when(mockReceiver.receiveMessages(anyInt(), any(Duration.class)))
                .thenReturn(new IterableStream<>(List.of(mockMsg)));

        List<LargeServiceBusMessage> received = client.receiveMessages(1);
        assertFalse(received.getFirst().getApplicationProperties()
                        .containsKey(LargeMessageClientConfiguration.LARGE_MESSAGE_CLIENT_USER_AGENT),
                "User-agent header should be stripped on receive");

        logger.info("✓ User-agent injected on send and stripped on receive");
    }

    // =========================================================================
    // Property type preservation
    // =========================================================================

    @Test
    @Order(12)
    @DisplayName("Multiple property types are preserved on send")
    void testPropertyTypePreservation() {
        LargeMessageClientConfiguration config = createTestConfiguration();
        AzureServiceBusLargeMessageClient client = new AzureServiceBusLargeMessageClient(
                mockSender, mockReceiver, mockPayloadStore, config);

        Map<String, Object> props = new HashMap<>();
        props.put("stringProp", "hello");
        props.put("intProp", 42);
        props.put("longProp", 123456789L);
        props.put("boolProp", true);
        props.put("doubleProp", 3.14);

        client.sendMessage(generateSmallMessage(), props);

        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(mockSender).sendMessage(captor.capture());

        Map<String, Object> captured = captor.getValue().getApplicationProperties();
        assertEquals("hello", captured.get("stringProp"));
        assertEquals(42, captured.get("intProp"));
        assertEquals(123456789L, captured.get("longProp"));
        assertEquals(true, captured.get("boolProp"));
        assertEquals(3.14, captured.get("doubleProp"));

        logger.info("✓ All property types preserved correctly");
    }
}
