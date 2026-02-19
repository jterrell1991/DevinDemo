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
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.header.Header;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceActionIntegrationTest {

  private static final String TOPIC_NAME = "topic-1";
  private static final String DEFAULT_KEY_SUBJECT = "topic-1-key";
  private static final String DEFAULT_VALUE_SUBJECT = "topic-1-value";

  @Rule
  public final DefaultKafkaRestTestEnvironment testEnv = new DefaultKafkaRestTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnv.kafkaCluster().createTopic(TOPIC_NAME, 3, (short) 1);
    Thread.sleep(1000);
  }

  @Test
  public void produceBinary() throws Exception {
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
  }

  @Test
  public void produceBinaryWithNullData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
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
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceBinaryWithInvalidData_throwsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(IntNode.valueOf(1))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(TextNode.valueOf("fooba")) // invalid base64 string
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

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceWithInvalidData_throwsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"records\": {\"subject\": \"foobar\" } }";

    Response response =
        testEnv
            .kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
    assertEquals(
        "Unrecognized field \"records\" (class io.confluent.kafkarest.entities.v3.AutoValue_ProduceRequest$Builder), not marked as ignorable (6 known properties: \"value\", \"originalSize\", \"partitionId\", \"headers\", \"key\", \"timestamp\"])",
        actual.getMessage());
  }

  @Test
  public void produceJson() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
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
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonWithNullData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(NullNode.getInstance())
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(NullNode.getInstance())
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
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceAvroWithRawSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithRawSchemaAndNullData_throwsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(NullNode.getInstance())
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(NullNode.getInstance())
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
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceAvroWithRawSchemaAndInvalidData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(1))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(2))
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

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithSchemaId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(
                DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(
                DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaId(keySchema.getSchemaId())
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaId(valueSchema.getSchemaId())
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithSchemaVersion() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(
                DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(
                DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaVersion(keySchema.getSchemaVersion())
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaVersion(valueSchema.getSchemaVersion())
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithLatestSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv.schemaRegistry()
        .createSchema(
            DEFAULT_KEY_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    testEnv.schemaRegistry()
        .createSchema(
            DEFAULT_VALUE_SUBJECT, new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithRawSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject("my-key-subject")
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject("my-value-subject")
                    .setRawSchema("{\"type\": \"string\"}")
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithSchemaIdAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(
                "my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(
                "my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setSchemaId(keySchema.getSchemaId())
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
                    .setSchemaId(valueSchema.getSchemaId())
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithSchemaVersionAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(
                "my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(
                "my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setSchemaVersion(keySchema.getSchemaVersion())
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
                    .setSchemaVersion(valueSchema.getSchemaVersion())
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithLatestSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv.schemaRegistry()
        .createSchema(
            "my-key-subject", new AvroSchema("{\"type\": \"string\"}"));
    testEnv.schemaRegistry()
        .createSchema(
            "my-value-subject", new AvroSchema("{\"type\": \"string\"}"));
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceAvroWithRawSchemaAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", \"type\": "
            + "\"string\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", \"type\": "
            + "\"string\"}]}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
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
    GenericRecord expectedKey = new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @Test
  public void produceAvroWithSchemaIdAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @Test
  public void produceAvroWithSchemaVersionAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setSchemaVersion(keySchemaKey.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setSchemaVersion(valueSchemaKey.getSchemaVersion())
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
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @Test
  public void produceAvroWithLatestSchemaAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    AvroSchema keySchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyKey\", \"fields\": [{\"name\": \"foo\", "
                + "\"type\": \"string\"}]}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    AvroSchema valueSchema =
        new AvroSchema(
            "{\"type\": \"record\", \"name\": \"MyValue\", \"fields\": [{\"name\": \"bar\", "
                + "\"type\": \"string\"}]}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
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
    GenericRecord expectedKey = new GenericData.Record(keySchema.rawSchema());
    expectedKey.put("foo", "foz");
    GenericRecord expectedValue = new GenericData.Record(valueSchema.rawSchema());
    expectedValue.put("bar", "baz");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @Test
  public void produceJsonschemaWithRawSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithRawSchemaAndNullData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(NullNode.getInstance())
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(NullNode.getInstance())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceJsonschemaWithRawSchemaAndInvalidData_throwsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(1))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(2))
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

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceJsonschemaWithSchemaId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(DEFAULT_KEY_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(DEFAULT_VALUE_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaId(keySchema.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaId(valueSchema.getSchemaId())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithSchemaVersion() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema(DEFAULT_KEY_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema(DEFAULT_VALUE_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaVersion(keySchema.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaVersion(valueSchema.getSchemaVersion())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithLatestSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv.schemaRegistry()
        .createSchema(DEFAULT_KEY_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    testEnv.schemaRegistry()
        .createSchema(DEFAULT_VALUE_SUBJECT, new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithRawSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject("my-key-subject")
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject("my-value-subject")
                    .setRawSchema("{\"type\": \"string\"}")
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithSchemaIdAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema("my-key-subject", new JsonSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema("my-value-subject", new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setSchemaId(keySchema.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
                    .setSchemaId(valueSchema.getSchemaId())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithSchemaVersionAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    SchemaKey keySchema =
        testEnv.schemaRegistry()
            .createSchema("my-key-subject", new JsonSchema("{\"type\": \"string\"}"));
    SchemaKey valueSchema =
        testEnv.schemaRegistry()
            .createSchema("my-value-subject", new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setSchemaVersion(keySchema.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
                    .setSchemaVersion(valueSchema.getSchemaVersion())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithLatestSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    testEnv.schemaRegistry()
        .createSchema("my-key-subject", new JsonSchema("{\"type\": \"string\"}"));
    testEnv.schemaRegistry()
        .createSchema("my-value-subject", new JsonSchema("{\"type\": \"string\"}"));
    TextNode key = TextNode.valueOf("foo");
    TextNode value = TextNode.valueOf("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject("my-key-subject")
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject("my-value-subject")
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithRawSchemaAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String keyRawSchema =
        "{\"type\": \"object\", \"title\": \"MyKey\", \"properties\": {\"foo\": "
            + "{\"type\": \"string\"}}}";
    String valueRawSchema =
        "{\"type\": \"object\", \"title\": \"MyValue\", \"properties\": {\"bar\": "
            + "{\"type\": \"string\"}}}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithSchemaIdAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    JsonSchema keySchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyKey\", \"properties\": {\"foo\": "
                + "{\"type\": \"string\"}}}");
    String keySubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    JsonSchema valueSchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyValue\", \"properties\": {\"bar\": "
                + "{\"type\": \"string\"}}}");
    String valueSubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithSchemaVersionAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    JsonSchema keySchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyKey\", \"properties\": {\"foo\": "
                + "{\"type\": \"string\"}}}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    JsonSchema valueSchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyValue\", \"properties\": {\"bar\": "
                + "{\"type\": \"string\"}}}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setSchemaVersion(keySchemaKey.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setSchemaVersion(valueSchemaKey.getSchemaVersion())
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceJsonschemaWithLatestSchemaAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    JsonSchema keySchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyKey\", \"properties\": {\"foo\": "
                + "{\"type\": \"string\"}}}");
    String keySubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    JsonSchema valueSchema =
        new JsonSchema(
            "{\"type\": \"object\", \"title\": \"MyValue\", \"properties\": {\"bar\": "
                + "{\"type\": \"string\"}}}");
    String valueSubject =
        new TopicNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void produceProtobufWithRawSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(keySchema.canonicalString())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(valueSchema.canonicalString())
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithRawSchemaAndNullData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(keySchema.canonicalString())
                    .setData(NullNode.getInstance())
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(valueSchema.canonicalString())
                    .setData(NullNode.getInstance())
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
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceProtobufWithRawSchemaAndInvalidData_throwsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(keySchema.canonicalString())
                    .setData(IntNode.valueOf(1))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(valueSchema.canonicalString())
                    .setData(IntNode.valueOf(2))
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

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceProtobufWithSchemaId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(DEFAULT_KEY_SUBJECT, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    SchemaKey valueSchemaKey =
        testEnv.schemaRegistry().createSchema(DEFAULT_VALUE_SUBJECT, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithSchemaVersion() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(DEFAULT_KEY_SUBJECT, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    SchemaKey valueSchemaKey =
        testEnv.schemaRegistry().createSchema(DEFAULT_VALUE_SUBJECT, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSchemaVersion(keySchemaKey.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSchemaVersion(valueSchemaKey.getSchemaVersion())
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithLatestSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    testEnv.schemaRegistry().createSchema(DEFAULT_KEY_SUBJECT, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    testEnv.schemaRegistry().createSchema(DEFAULT_VALUE_SUBJECT, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithRawSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject("my-key-subject")
                    .setRawSchema(keySchema.canonicalString())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject("my-value-subject")
                    .setRawSchema(valueSchema.canonicalString())
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithSchemaIdAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    String keySubject = "my-key-schema";
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    String valueSubject = "my-value-schema";
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject(keySubject)
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject(valueSubject)
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithSchemaVersionAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    String keySubject = "my-key-schema";
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    String valueSubject = "my-value-schema";
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject(keySubject)
                    .setSchemaVersion(keySchemaKey.getSchemaVersion())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject(valueSubject)
                    .setSchemaVersion(valueSchemaKey.getSchemaVersion())
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithLatestSchemaAndSubject() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    String keySubject = "my-key-subject";
    testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    String valueSubject = "my-value-subject";
    testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubject(keySubject)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubject(valueSubject)
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithRawSchemaAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(keySchema.canonicalString())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(valueSchema.canonicalString())
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceProtobufWithSchemaIdAndSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyKey { string foo = 1; }");
    String keySubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ true, keySchema);
    SchemaKey keySchemaKey = testEnv.schemaRegistry().createSchema(keySubject, keySchema);
    ProtobufSchema valueSchema =
        new ProtobufSchema("syntax = \"proto3\"; message MyValue { string bar = 1; }");
    String valueSubject =
        new RecordNameStrategy().subjectName(TOPIC_NAME, /* isKey= */ false, valueSchema);
    SchemaKey valueSchemaKey = testEnv.schemaRegistry().createSchema(valueSubject, valueSchema);
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("foo", "foz");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("bar", "baz");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setSchemaId(keySchemaKey.getSchemaId())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
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
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("foo"), "foz");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("bar"), "baz");
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceBinaryWithPartitionId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int partitionId = 1;
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(partitionId)
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
                partitionId,
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void produceBinaryWithTimestamp() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1000);
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
            .setTimestamp(timestamp)
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
  }

  @Test
  public void produceBinaryWithHeaders() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-1")),
                    ProduceRequestHeader.create("header-1", ByteString.copyFromUtf8("value-2")),
                    ProduceRequestHeader.create("header-2", ByteString.copyFromUtf8("value-3"))))
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
        Arrays.asList(
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-1").toByteArray()),
            new RecordHeader("header-1", ByteString.copyFromUtf8("value-2").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-1")));
    assertEquals(
        singletonList(
            new RecordHeader("header-2", ByteString.copyFromUtf8("value-3").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("header-2")));
  }

  @Test
  public void produceBinaryAndAvro() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
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
    ConsumerRecord<byte[], Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, produced.value());
  }

  @Test
  public void produceBinaryKeyOnly() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(key.toByteArray()))
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
    assertNull(produced.value());
  }

  @Test
  public void produceBinaryValueOnly() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
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
    assertNull(produced.key());
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void produceNothing() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request = ProduceRequest.builder().build();

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
    assertNull(produced.key());
    assertNull(produced.value());
  }

  @Test
  public void produceJsonBatch() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 1000; i ++) {
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("value-" + i))
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
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    for (int i = 0; i < 1000; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  TOPIC_NAME,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals(
          requests.get(i)
              .getKey()
              .map(ProduceRequestData::getData)
              .map(JsonNode::asText)
              .orElse(null),
          produced.key());
      assertEquals(
          requests.get(i)
              .getValue()
              .map(ProduceRequestData::getData)
              .map(JsonNode::asText)
              .orElse(null),
          produced.value());
    }
  }

  @Test
  public void produceBinaryBatchWithInvalidData_throwsMultipleBadRequests() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 1000; i ++) {
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(2 * i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(2 * i + 1))
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

    List<ErrorResponse> actual = readErrorResponses(response);
    for (int i = 0; i < 1000; i++) {
      assertEquals(400, actual.get(i).getErrorCode());
    }
  }

  @Test
  public void produceBinaryWithSchemaSubject_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"type\": \"BINARY\", \"subject\": \"foobar\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBinaryWithSchemaSubjectStrategy_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"type\": \"BINARY\", \"subject_name_strategy\": \"TOPIC\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBinaryWithRawSchema_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"type\": \"BINARY\", \"schema\": \"{ \\\"type\\\": \\\"string\\\" }\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBinaryWithSchemaId_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"type\": \"BINARY\", \"schema_id\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBinaryWithSchemaVersion_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"type\": \"BINARY\", \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithTypeAndSchemaVersion_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"key\": { \"type\": \"AVRO\", \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithTypeAndSchemaId_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"key\": { \"type\": \"AVRO\", \"schema_id\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithTypeAndLatestSchema_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"key\": { \"type\": \"AVRO\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithSchemaSubjectAndSchemaSubjectStrategy_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"subject\": \"foobar\", \"subject_name_strategy\": \"TOPIC\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithSchemaIdAndSchemaVersion_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"key\": { \"schema_id\": 1, \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithRawSchemaAndSchemaId_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"schema\": \"{ \\\"type\\\": \\\"string\\\" }\", \"schema_id\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithRawSchemaAndSchemaVersion_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"schema\": \"{ \\\"type\\\": \\\"string\\\" }\", \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithRecordSchemaSubjectStrategyAndSchemaVersion_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request =
        "{ \"key\": { \"subject_name_strategy\": \"RECORD_NAME\", \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceAvroWithRecordSchemaSubjectStrategyAndLatestVersion_returnsBadRequest()
      throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String request = "{ \"key\": { \"subject_name_strategy\": \"RECORD_NAME\" } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceJsonWithPartitionId() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int partitionId = 2;
    String key = "foo";
    String value = "bar";
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
  public void produceBinaryWithPartitionIdAllPartitions() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    for (int partitionId = 0; partitionId < 3; partitionId++) {
      ByteString key = ByteString.copyFromUtf8("key-p" + partitionId);
      ByteString value = ByteString.copyFromUtf8("value-p" + partitionId);
      ProduceRequest request =
          ProduceRequest.builder()
              .setPartitionId(partitionId)
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
      assertEquals(partitionId, actual.getPartitionId());
      ConsumerRecord<byte[], byte[]> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  TOPIC_NAME,
                  partitionId,
                  actual.getOffset(),
                  new ByteArrayDeserializer(),
                  new ByteArrayDeserializer());
      assertEquals(key, ByteString.copyFrom(produced.key()));
      assertEquals(value, ByteString.copyFrom(produced.value()));
    }
  }

  @Test
  public void produceJsonWithTimestamp() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(1000);
    String key = "foo";
    String value = "bar";
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
                    .setData(TextNode.valueOf(value))
                    .build())
            .setTimestamp(timestamp)
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
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
  }

  @Test
  public void produceBinaryWithZeroTimestamp() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(0);
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
            .setTimestamp(timestamp)
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
  }

  @Test
  public void produceBinaryWithFutureTimestamp() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(4102444800000L);
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
            .setTimestamp(timestamp)
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
  }

  @Test
  public void produceBinaryWithEmptyHeaders() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(ImmutableList.of())
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
    assertEquals(0, produced.headers().toArray().length);
  }

  @Test
  public void produceBinaryWithNullHeaderValue() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(
                singletonList(
                    ProduceRequestHeader.create("header-with-null", null)))
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
    Header header = produced.headers().headers("header-with-null").iterator().next();
    assertEquals("header-with-null", header.key());
    assertNull(header.value());
  }

  @Test
  public void produceBinaryWithManyHeaders() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ByteString key = ByteString.copyFromUtf8("foo");
    ByteString value = ByteString.copyFromUtf8("bar");
    List<ProduceRequestHeader> headers = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      headers.add(
          ProduceRequestHeader.create(
              "header-" + i, ByteString.copyFromUtf8("value-" + i)));
    }
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(headers)
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
    assertEquals(50, produced.headers().toArray().length);
    for (int i = 0; i < 50; i++) {
      assertEquals(
          singletonList(
              new RecordHeader(
                  "header-" + i,
                  ByteString.copyFromUtf8("value-" + i).toByteArray())),
          ImmutableList.copyOf(produced.headers().headers("header-" + i)));
    }
  }

  @Test
  public void produceBinaryWithLargePayload() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    byte[] largeKeyBytes = new byte[10000];
    byte[] largeValueBytes = new byte[100000];
    Arrays.fill(largeKeyBytes, (byte) 'K');
    Arrays.fill(largeValueBytes, (byte) 'V');
    ByteString key = ByteString.copyFrom(largeKeyBytes);
    ByteString value = ByteString.copyFrom(largeValueBytes);
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
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void produceJsonSmallBatch() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("value-" + i))
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
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), false);

    for (int i = 0; i < 10; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  TOPIC_NAME,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("key-" + i, produced.key());
      assertEquals("value-" + i, produced.value());
    }
  }

  @Test
  public void produceBatchMixedValidAndInvalid() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String validRequest =
        "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"}}";
    String invalidRequest =
        "{\"key\":{\"type\":\"BINARY\",\"data\":1},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":2}}";
    String batch = validRequest + invalidRequest + validRequest;

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    response.bufferEntity();
    String body = response.readEntity(String.class);
    ObjectMapper mapper = testEnv.kafkaRest().getObjectMapper();
    MappingIterator<JsonNode> iter =
        mapper.readerFor(JsonNode.class).readValues(body);
    List<JsonNode> results = new ArrayList<>();
    while (iter.hasNext()) {
      results.add(iter.next());
    }
    assertEquals(3, results.size());
    assertEquals(200, results.get(0).path("error_code").asInt(200));
    assertEquals(400, results.get(1).path("error_code").asInt(200));
    assertEquals(200, results.get(2).path("error_code").asInt(200));
  }

  @Test
  public void produceAvroWithBackwardCompatibleSchemaEvolution() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String schemaV1 =
        "{\"type\":\"record\",\"name\":\"TestRecord\","
            + "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    String schemaV2 =
        "{\"type\":\"record\",\"name\":\"TestRecord\","
            + "\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},"
            + "{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    ObjectNode dataV1 = JsonNodeFactory.instance.objectNode();
    dataV1.put("field1", "hello");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV1)
                    .setData(dataV1)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV1)
                    .setData(dataV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);

    ObjectNode dataV2 = JsonNodeFactory.instance.objectNode();
    dataV2.put("field1", "world");
    dataV2.putObject("field2").put("string", "extra");
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV2)
                    .setData(dataV2)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV2)
                    .setData(dataV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);

    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord keyV1 = (GenericRecord) producedV1.key();
    assertEquals("hello", keyV1.get("field1").toString());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord keyV2 = (GenericRecord) producedV2.key();
    assertEquals("world", keyV2.get("field1").toString());
    assertEquals("extra", keyV2.get("field2").toString());
  }

  @Test
  public void produceToNonExistentTopic_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("foo").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("bar").toByteArray()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/nonexistent-topic/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(40301, actual.getErrorCode());
  }

  @Test
  public void produceWithInvalidClusterId_returnsNotFound() throws Exception {
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("foo").toByteArray()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/invalid-cluster-id/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void produceBinaryWithPartitionIdAndTimestampAndHeaders() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    int partitionId = 0;
    Instant timestamp = Instant.ofEpochMilli(5000);
    ByteString key = ByteString.copyFromUtf8("combo-key");
    ByteString value = ByteString.copyFromUtf8("combo-value");
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(partitionId)
            .setTimestamp(timestamp)
            .setHeaders(
                Arrays.asList(
                    ProduceRequestHeader.create("h1", ByteString.copyFromUtf8("v1")),
                    ProduceRequestHeader.create("h2", ByteString.copyFromUtf8("v2"))))
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
    assertEquals(partitionId, actual.getPartitionId());
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                TOPIC_NAME,
                partitionId,
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
    assertEquals(
        singletonList(
            new RecordHeader("h1", ByteString.copyFromUtf8("v1").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("h1")));
    assertEquals(
        singletonList(
            new RecordHeader("h2", ByteString.copyFromUtf8("v2").toByteArray())),
        ImmutableList.copyOf(produced.headers().headers("h2")));
  }

  @Test
  public void produceAvroWithTimestamp() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    Instant timestamp = Instant.ofEpochMilli(2000);
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setTimestamp(timestamp)
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
    assertEquals(value, produced.value());
    assertEquals(timestamp, Instant.ofEpochMilli(produced.timestamp()));
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
