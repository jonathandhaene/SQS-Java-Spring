package com.azure.servicebus.extended.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

/**
 * Represents a pointer to a message payload stored in Azure Blob Storage.
 */
public class BlobPointer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String containerName;
    private final String blobName;

    @JsonCreator
    public BlobPointer(
            @JsonProperty("containerName") String containerName,
            @JsonProperty("blobName") String blobName) {
        this.containerName = containerName;
        this.blobName = blobName;
    }

    @JsonProperty("containerName")
    public String getContainerName() {
        return containerName;
    }

    @JsonProperty("blobName")
    public String getBlobName() {
        return blobName;
    }

    /**
     * Serializes this BlobPointer to JSON string.
     *
     * @return JSON representation of the blob pointer
     */
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize BlobPointer to JSON", e);
        }
    }

    /**
     * Deserializes a BlobPointer from JSON string.
     *
     * @param json JSON representation of the blob pointer
     * @return BlobPointer instance
     */
    public static BlobPointer fromJson(String json) {
        try {
            return objectMapper.readValue(json, BlobPointer.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize BlobPointer from JSON", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlobPointer that = (BlobPointer) o;
        return Objects.equals(containerName, that.containerName) &&
                Objects.equals(blobName, that.blobName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerName, blobName);
    }

    @Override
    public String toString() {
        return "BlobPointer{" +
                "containerName='" + containerName + '\'' +
                ", blobName='" + blobName + '\'' +
                '}';
    }
}
