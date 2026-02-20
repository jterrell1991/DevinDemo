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
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.EnumSubjectNameStrategy;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestHeader;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.testing.DefaultKafkaRestTestEnvironment;
import io.confluent.kafkarest.testing.SchemaRegistryFixture.SchemaKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore
@RunWith(JUnit4.class)
public class ProduceActionExtendedIntegrationTest {

  private static final String TOPIC_NAME = "topic-1";

  @Rule
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnv.kafkaCluster().createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @Test
  public void produceJsonWithNestedObjectData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 42);
    key.put("name", "test-key");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("status", "active");
    ObjectNode nested = value.putObject("metadata");
    nested.put("version", 1);
    nested.put("source", "integration-test");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(key)
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
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertNotNull(produced.key());
    assertNotNull(produced.value());
  }

  @Test
  public void produceBinaryToNonExistentTopic_throwsNotFound() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
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
            .path(
                "/v3/clusters/" + clusterId + "/topics/non-existent-topic/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertTrue(actual.getErrorCode() >= 400);
  }

  @Test
  public void produceWithInvalidClusterId_throwsNotFound() throws Exception {
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
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
            .path(
                "/v3/clusters/invalid-cluster-id/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertTrue(response.getStatus() >= 400);
  }

  @Test
  public void produceJsonWithPartitionId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int partitionId = 2;
    String key = "partitioned-key";
    String value = "partitioned-value";
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(partitionId)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(value))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertEquals(partitionId, actual.getPartitionId());
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), false);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                partitionId,
                actual.getOffset(),
                deserializer,
                deserializer);
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroKeyOnly() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "avro-key-only";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertEquals(key, produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceJsonValueOnly() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String value = "json-value-only";
    ProduceRequest request =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf(value))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertNull(produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceBinaryWithTimestampAndHeaders() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1234567890L);
    ByteString key = ByteString.copyFromUtf8("ts-header-key");
    ByteString value = ByteString.copyFromUtf8("ts-header-value");
    ProduceRequest request =
        ProduceRequest.builder()
            .setTimestamp(timestamp)
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create(
                        "trace-id", ByteString.copyFromUtf8("abc-123")),
                    ProduceRequestHeader.create(
                        "correlation-id", ByteString.copyFromUtf8("xyz-789"))))
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
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
    assertEquals(
        singletonList(
            new RecordHeader(
                "trace-id",
                ByteString.copyFromUtf8("abc-123").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("trace-id")));
    assertEquals(
        singletonList(
            new RecordHeader(
                "correlation-id",
                ByteString.copyFromUtf8("xyz-789").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("correlation-id")));
  }

  @Test
  public void produceResponseContainsCorrectMetadata() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("metadata-key");
    ByteString value = ByteString.copyFromUtf8("metadata-value");
    Instant timestamp = Instant.ofEpochMilli(5000);
    ProduceRequest request =
        ProduceRequest.builder()
            .setTimestamp(timestamp)
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
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertEquals(clusterId, actual.getClusterId());
    assertEquals(TOPIC_NAME, actual.getTopicName());
    assertTrue(actual.getPartitionId() >= 0 && actual.getPartitionId() < 3);
    assertTrue(actual.getOffset() >= 0);
    assertTrue(actual.getTimestamp().isPresent());
    assertEquals(timestamp, actual.getTimestamp().get());
  }

  @Test
  public void produceBinaryBatch() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ByteString batchKey = ByteString.copyFromUtf8("batch-key-" + i);
      ByteString batchValue = ByteString.copyFromUtf8("batch-value-" + i);
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(BinaryNode.valueOf(batchKey.toByteArray()))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(BinaryNode.valueOf(batchValue.toByteArray()))
                      .build())
              .build());
    }

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = testEnv.kafkaRest().getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(10, actual.size());
    for (int i = 0; i < 10; i++) {
      ConsumerRecord<byte[], byte[]> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  TOPIC_NAME,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  new ByteArrayDeserializer(),
                  new ByteArrayDeserializer());
      assertEquals(
          ByteString.copyFromUtf8("batch-key-" + i),
          ByteString.copyFrom(produced.key()));
      assertEquals(
          ByteString.copyFromUtf8("batch-value-" + i),
          ByteString.copyFrom(produced.value()));
    }
  }

  @Test
  public void produceMultipleSequentialRecords() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int recordCount = 5;
    List<ProduceResponse> responses = new ArrayList<>();

    for (int i = 0; i < recordCount; i++) {
      ByteString key = ByteString.copyFromUtf8("seq-key-" + i);
      ByteString value = ByteString.copyFromUtf8("seq-value-" + i);
      ProduceRequest request =
          ProduceRequest.builder()
              .setPartitionId(0)
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
              .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .post(Entity.entity(request, MediaType.APPLICATION_JSON));
      assertEquals(Status.OK.getStatusCode(), response.getStatus());
      responses.add(readProduceResponse(response));
    }

    for (int i = 0; i < recordCount; i++) {
      ProduceResponse resp = responses.get(i);
      assertEquals(0, resp.getPartitionId());
      ConsumerRecord<byte[], byte[]> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  TOPIC_NAME,
                  resp.getPartitionId(),
                  resp.getOffset(),
                  new ByteArrayDeserializer(),
                  new ByteArrayDeserializer());
      assertEquals(
          ByteString.copyFromUtf8("seq-key-" + i),
          ByteString.copyFrom(produced.key()));
      assertEquals(
          ByteString.copyFromUtf8("seq-value-" + i),
          ByteString.copyFrom(produced.value()));
    }

    for (int i = 1; i < recordCount; i++) {
      assertTrue(responses.get(i).getOffset() > responses.get(i - 1).getOffset());
    }
  }

  @Test
  public void produceAvroWithComplexNestedRecord() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"Order\", \"fields\": ["
            + "{\"name\": \"orderId\", \"type\": \"string\"},"
            + "{\"name\": \"amount\", \"type\": \"int\"},"
            + "{\"name\": \"address\", \"type\": {\"type\": \"record\","
            + " \"name\": \"Address\", \"fields\": ["
            + "{\"name\": \"street\", \"type\": \"string\"},"
            + "{\"name\": \"city\", \"type\": \"string\"}"
            + "]}}"
            + "]}";
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("orderId", "ORD-001");
    value.put("amount", 100);
    ObjectNode address = value.putObject("address");
    address.put("street", "123 Main St");
    address.put("city", "Springfield");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("order-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(valueRawSchema)
                    .setData(value)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertEquals("order-key", produced.key());
    GenericRecord producedValue = (GenericRecord) produced.value();
    assertEquals("ORD-001", producedValue.get("orderId").toString());
    assertEquals(100, producedValue.get("amount"));
    GenericRecord producedAddress = (GenericRecord) producedValue.get("address");
    assertEquals("123 Main St", producedAddress.get("street").toString());
    assertEquals("Springfield", producedAddress.get("city").toString());
  }

  @Test
  public void produceBinaryWithHeadersContainingNullValue() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("null-header-key");
    ByteString value = ByteString.copyFromUtf8("null-header-value");
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create("header-with-value", ByteString.copyFromUtf8("v1")),
                    ProduceRequestHeader.create("header-without-value", null)))
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
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(
        singletonList(
            new RecordHeader(
                "header-with-value",
                ByteString.copyFromUtf8("v1").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-with-value")));
    assertEquals(
        singletonList(new RecordHeader("header-without-value", null)),
        ImmutableList.copyOf(produced.headers().headers("header-without-value")));
  }

  @Test
  public void produceProtobufWithTopicRecordNameStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    String keySubject =
        new TopicRecordNameStrategy().subjectName(TOPIC_NAME, true, keySchema);
    String valueSubject =
        new TopicRecordNameStrategy().subjectName(TOPIC_NAME, false, valueSchema);
    SchemaKey keySchemaKey =
        testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    SchemaKey valueSchemaKey =
        testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_RECORD_NAME)
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_RECORD_NAME)
                    .setSchemaId(valueSchemaKey.getSchemaId())
                    .setData(value)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Message, Message> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedKey =
        DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue =
        DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceBinaryWithEmptyData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFrom(new byte[0]);
    ByteString value = ByteString.copyFrom(new byte[0]);
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
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
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
    assertEquals(0, produced.key().length);
    assertEquals(0, produced.value().length);
  }

  private static ProduceResponse readProduceResponse(Response response) {
    response.bufferEntity();
    try {
      return response.readEntity(ProduceResponse.class);
    } catch (ProcessingException e) {
      throw new RuntimeException(response.readEntity(ErrorResponse.class).toString(), e);
    }
  }

  private static ImmutableList<ProduceResponse> readProduceResponses(Response response) {
    return ImmutableList.copyOf(
        response.readEntity(new GenericType<MappingIterator<ProduceResponse>>() {}));
  }

  private static ImmutableList<ErrorResponse> readErrorResponses(Response response) {
    return ImmutableList.copyOf(
        response.readEntity(new GenericType<MappingIterator<ErrorResponse>>() {}));
  }
}
