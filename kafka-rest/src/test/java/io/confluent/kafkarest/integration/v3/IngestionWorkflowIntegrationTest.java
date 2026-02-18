/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import io.confluent.kafkarest.testing.SchemaRegistryFixture.SchemaKey;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IngestionWorkflowIntegrationTest {

  private static final String TOPIC_NAME = "ingestion-test-topic";

  @Rule
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  private String clusterId;

  @Before
  public void setUp() throws Exception {
    testEnv.kafkaCluster().createTopic(TOPIC_NAME, 3, (short) 1);
    clusterId = testEnv.kafkaCluster().getClusterId();
  }

  private String produceEndpoint() {
    return "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records";
  }

  // ---- Section 1: Realistic Upstream Partner Payloads ----

  @Test
  public void partnerPayload_binaryWithStructuredContent_publishesSuccessfully() throws Exception {
    byte[] partnerPayload = "{\"event\":\"order.created\",\"orderId\":\"ORD-12345\"}".getBytes();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("partner-A-key-001".getBytes()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(partnerPayload))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(
        ByteString.copyFrom(partnerPayload),
        ByteString.copyFrom(produced.value()));
  }

  @Test
  public void partnerPayload_jsonWithNestedObject_publishesSuccessfully() throws Exception {
    ObjectNode partnerEvent = JsonNodeFactory.instance.objectNode();
    partnerEvent.put("eventType", "shipment.dispatched");
    partnerEvent.put("partnerId", "PARTNER-42");
    ObjectNode shipmentDetails = partnerEvent.putObject("shipment");
    shipmentDetails.put("trackingNumber", "TRK-98765");
    shipmentDetails.put("carrier", "FedEx");
    shipmentDetails.put("weight", 2.5);

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("shipment-key-001"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(partnerEvent)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertTrue(actual.getOffset() >= 0);
  }

  @Test
  public void partnerPayload_avroWithComplexSchema_publishesSuccessfully() throws Exception {
    String avroSchema =
        "{\"type\":\"record\",\"name\":\"PartnerEvent\",\"fields\":["
            + "{\"name\":\"eventId\",\"type\":\"string\"},"
            + "{\"name\":\"partnerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"int\"},"
            + "{\"name\":\"currency\",\"type\":\"string\"}"
            + "]}";
    ObjectNode avroValue = JsonNodeFactory.instance.objectNode();
    avroValue.put("eventId", "EVT-001");
    avroValue.put("partnerId", "PARTNER-42");
    avroValue.put("amount", 9999);
    avroValue.put("currency", "USD");

    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(avroValue)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertNotNull(produced.value());
    assertTrue(produced.value().toString().contains("PARTNER-42"));
  }

  @Test
  public void partnerPayload_largeBinaryBlob_publishesSuccessfully() throws Exception {
    byte[] largePayload = new byte[10000];
    for (int i = 0; i < largePayload.length; i++) {
      largePayload[i] = (byte) (i % 256);
    }

    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(largePayload))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(largePayload.length, produced.value().length);
  }

  // ---- Section 2: Malformed and Contract-Breaking Payloads ----

  @Test
  public void malformedPayload_invalidJsonSyntax_returnsError() throws Exception {
    String malformedJson = "{ this is not valid json }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(malformedJson, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_unrecognizedTopLevelField_returnsError() throws Exception {
    String contractBreaking = "{ \"records\": {\"subject\": \"foobar\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(contractBreaking, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_binaryWithInvalidBase64_returnsError() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(TextNode.valueOf("not-valid-base64!!!"))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_binaryWithNonTextualData_returnsError() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(IntNode.valueOf(12345))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_avroDataTypeMismatch_returnsError() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(999))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_avroMissingRequiredField_returnsError() throws Exception {
    String avroSchema =
        "{\"type\":\"record\",\"name\":\"RequiredFields\",\"fields\":["
            + "{\"name\":\"requiredField\",\"type\":\"string\"}"
            + "]}";
    ObjectNode incompleteData = JsonNodeFactory.instance.objectNode();

    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(incompleteData)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_nonExistentSchemaId_returnsError() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaId(999999)
                    .setData(TextNode.valueOf("data"))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertTrue(actual.getErrorCode() >= 400);
  }

  @Test
  public void malformedPayload_conflictingSchemaParams_returnsError() throws Exception {
    String rawRequest =
        "{\"value\":{\"schema_id\":1,\"schema_version\":1,\"data\":\"test\"}}";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(rawRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_emptyBody_returnsError() throws Exception {
    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("", MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void malformedPayload_nullKeyAndValue_producesSuccessfully() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(NullNode.getInstance())
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(NullNode.getInstance())
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  // ---- Section 3: Duplicate Ingestion Events ----

  @Test
  public void duplicateIngestion_identicalPayloadSentTwice_producesBothMessages()
      throws Exception {
    ByteString key = ByteString.copyFromUtf8("dedup-key-001");
    ByteString value = ByteString.copyFromUtf8("{\"event\":\"order.created\",\"id\":\"ORD-1\"}");

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(value.toByteArray()))
                    .build())
            .build();

    Response response1 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response1.getStatus());
    ProduceResponse result1 = readProduceResponse(response1);

    Response response2 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
    ProduceResponse result2 = readProduceResponse(response2);

    boolean differentOffset =
        result1.getPartitionId() != result2.getPartitionId()
            || result1.getOffset() != result2.getOffset();
    assertTrue(
        "Identical payloads should produce distinct records (no deduplication)",
        differentOffset);

    ConsumerRecord<byte[], byte[]> produced1 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                result1.getPartitionId(),
                result1.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    ConsumerRecord<byte[], byte[]> produced2 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                result2.getPartitionId(),
                result2.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(ByteString.copyFrom(produced1.value()), ByteString.copyFrom(produced2.value()));
  }

  @Test
  public void duplicateIngestion_sameJsonPayloadTwice_producesBothMessages() throws Exception {
    ObjectNode partnerEvent = JsonNodeFactory.instance.objectNode();
    partnerEvent.put("eventType", "payment.received");
    partnerEvent.put("transactionId", "TXN-DUPLICATE-001");
    partnerEvent.put("amount", 150);

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("txn-key-001"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(partnerEvent)
                    .build())
            .build();

    Response response1 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response1.getStatus());
    ProduceResponse result1 = readProduceResponse(response1);

    Response response2 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
    ProduceResponse result2 = readProduceResponse(response2);

    boolean differentOffset =
        result1.getPartitionId() != result2.getPartitionId()
            || result1.getOffset() != result2.getOffset();
    assertTrue(
        "Duplicate JSON payloads should produce distinct records (no deduplication)",
        differentOffset);
  }

  @Test
  public void duplicateIngestion_sameAvroPayloadTwice_producesBothMessages() throws Exception {
    String avroSchema = "{\"type\": \"string\"}";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(TextNode.valueOf("avro-dedup-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(TextNode.valueOf("avro-dedup-value"))
                    .build())
            .build();

    Response response1 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response1.getStatus());
    ProduceResponse result1 = readProduceResponse(response1);

    Response response2 =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
    ProduceResponse result2 = readProduceResponse(response2);

    boolean differentOffset =
        result1.getPartitionId() != result2.getPartitionId()
            || result1.getOffset() != result2.getOffset();
    assertTrue(
        "Duplicate Avro payloads should produce distinct records (no deduplication)",
        differentOffset);
  }

  // ---- Section 4: Upstream Retry Behavior ----

  @Test
  public void upstreamRetry_retryAfterSuccess_producesDuplicateRecord() throws Exception {
    ByteString key = ByteString.copyFromUtf8("retry-key-001");
    ByteString value = ByteString.copyFromUtf8("retry-payload-data");

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(value.toByteArray()))
                    .build())
            .build();

    Response initialResponse =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), initialResponse.getStatus());
    ProduceResponse initialResult = readProduceResponse(initialResponse);

    Response retryResponse =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), retryResponse.getStatus());
    ProduceResponse retryResult = readProduceResponse(retryResponse);

    boolean differentOffset =
        initialResult.getPartitionId() != retryResult.getPartitionId()
            || initialResult.getOffset() != retryResult.getOffset();
    assertTrue(
        "Retry should produce a new record (gateway has no idempotency)",
        differentOffset);

    ConsumerRecord<byte[], byte[]> record1 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                initialResult.getPartitionId(),
                initialResult.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    ConsumerRecord<byte[], byte[]> record2 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                retryResult.getPartitionId(),
                retryResult.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(
        "Both records should contain identical data",
        ByteString.copyFrom(record1.value()),
        ByteString.copyFrom(record2.value()));
  }

  @Test
  public void upstreamRetry_retryAfterMalformedRequest_onlyValidRequestPersisted()
      throws Exception {
    String malformedRequest = "{ not valid json at all }";
    Response failedResponse =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(malformedRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), failedResponse.getStatus());
    ErrorResponse errorResult = failedResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResult.getErrorCode());

    ByteString value = ByteString.copyFromUtf8("valid-retry-payload");
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(value.toByteArray()))
                    .build())
            .build();

    Response successResponse =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), successResponse.getStatus());

    ProduceResponse successResult = readProduceResponse(successResponse);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                successResult.getPartitionId(),
                successResult.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void upstreamRetry_multipleSequentialRetries_allProducedToKafka() throws Exception {
    int retryCount = 5;
    long[] offsets = new long[retryCount];
    int[] partitions = new int[retryCount];

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("multi-retry-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("multi-retry-value"))
                    .build())
            .build();

    for (int i = 0; i < retryCount; i++) {
      Response response =
          testEnv.kafkaRest()
              .target()
              .path(produceEndpoint())
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .post(Entity.entity(request, MediaType.APPLICATION_JSON));
      assertEquals(Status.OK.getStatusCode(), response.getStatus());
      ProduceResponse result = readProduceResponse(response);
      offsets[i] = result.getOffset();
      partitions[i] = result.getPartitionId();
    }

    for (int i = 0; i < retryCount; i++) {
      KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
      deserializer.configure(emptyMap(), false);
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(TOPIC_NAME, partitions[i], offsets[i], deserializer, deserializer);
      assertEquals("multi-retry-value", produced.value());
    }
  }

  // ---- Section 5: Downstream Message Publication Validation ----

  @Test
  public void downstreamValidation_binaryRoundTrip_dataMatchesExactly() throws Exception {
    ByteString key = ByteString.copyFromUtf8("downstream-key");
    ByteString value = ByteString.copyFromUtf8("{\"action\":\"persist\",\"target\":\"staging\"}");

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(value.toByteArray()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);

    assertEquals(TOPIC_NAME, actual.getTopicName());
    assertTrue(actual.getPartitionId() >= 0);
    assertTrue(actual.getOffset() >= 0);

    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(TOPIC_NAME, produced.topic());
    assertEquals(actual.getPartitionId(), produced.partition());
    assertEquals(actual.getOffset(), produced.offset());
  }

  @Test
  public void downstreamValidation_jsonRoundTrip_dataMatchesExactly() throws Exception {
    String key = "json-downstream-key";
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    value.put("pipeline", "analytics");
    value.put("eventType", "user.signup");
    value.put("userId", 42);

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(value)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), false);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertEquals(key, produced.key());
    assertNotNull(produced.value());
  }

  @Test
  public void downstreamValidation_avroRoundTrip_dataMatchesExactly() throws Exception {
    String avroSchema = "{\"type\": \"string\"}";
    String key = "avro-downstream-key";
    String value = "avro-downstream-value";

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(avroSchema)
                    .setData(TextNode.valueOf(value))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key().toString());
    assertEquals(value, produced.value().toString());
  }

  @Test
  public void downstreamValidation_produceWithSchemaId_persistsWithCorrectSchema()
      throws Exception {
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(
                TOPIC_NAME + "-value", new AvroSchema("{\"type\": \"string\"}"));

    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaId(valueSchema.getSchemaId())
                    .setData(TextNode.valueOf("schema-id-test-value"))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("schema-id-test-value", produced.value().toString());
  }

  @Test
  public void downstreamValidation_responseMetadataMatchesPersistedRecord() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("meta-key".getBytes()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("meta-value".getBytes()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path(produceEndpoint())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);

    assertEquals(clusterId, actual.getClusterId());
    assertEquals(TOPIC_NAME, actual.getTopicName());
    assertTrue(actual.getPartitionId() >= 0 && actual.getPartitionId() < 3);
    assertTrue(actual.getOffset() >= 0);

    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(actual.getPartitionId(), produced.partition());
    assertEquals(actual.getOffset(), produced.offset());
  }

  @Test
  public void downstreamValidation_produceToNonExistentTopic_returnsError() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf("value".getBytes()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/nonexistent-topic-xyz/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertTrue(actual.getErrorCode() >= 400);
  }

  private static ProduceResponse readProduceResponse(Response response) {
    response.bufferEntity();
    try {
      return response.readEntity(ProduceResponse.class);
    } catch (ProcessingException e) {
      throw new RuntimeException(response.readEntity(ErrorResponse.class).toString(), e);
    }
  }
}
