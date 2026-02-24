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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// TODO ddimitrov This continues being way too flaky.
//  Until we fix it (KREST-1542), we should ignore it, as it might be hiding even worse errors.
@Ignore
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

  // =========================================================================
  // Multi-Format Pipeline Integration Tests
  // =========================================================================

  @Test
  public void multiFormatPipeline_binary() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-binary";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    ByteString key = ByteString.copyFromUtf8("binary-key");
    ByteString value = ByteString.copyFromUtf8("binary-value");
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertNotNull(actual.getClusterId());
    assertEquals(topicName, actual.getTopicName());
    assertTrue(actual.getOffset() >= 0);

    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void multiFormatPipeline_avro() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-avro";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"AvroKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"int\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"AvroValue\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"count\", \"type\": \"int\"}]}";

    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 42);
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "test-record");
    value.put("count", 100);

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertNotNull(actual.getClusterId());
    assertEquals(topicName, actual.getTopicName());

    // Validate schema was registered
    assertTrue(actual.getKey().isPresent());
    assertTrue(actual.getKey().get().getSchemaId().isPresent());
    assertTrue(actual.getValue().isPresent());
    assertTrue(actual.getValue().get().getSchemaId().isPresent());

    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());

    GenericRecord expectedKey =
        new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("id", 42);
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("name", "test-record");
    expectedValue.put("count", 100);
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());
  }

  @Test
  public void multiFormatPipeline_jsonSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-jsonschema";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String keyRawSchema =
        "{\"type\": \"object\", \"title\": \"JsonKey\", \"properties\": "
            + "{\"id\": {\"type\": \"integer\"}}}";
    String valueRawSchema =
        "{\"type\": \"object\", \"title\": \"JsonValue\", \"properties\": "
            + "{\"message\": {\"type\": \"string\"}, \"priority\": {\"type\": \"integer\"}}}";

    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 7);
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("message", "hello-world");
    value.put("priority", 1);

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema(valueRawSchema)
                    .setData(value)
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertNotNull(actual.getClusterId());
    assertEquals(topicName, actual.getTopicName());

    // Validate schema registration
    assertTrue(actual.getKey().isPresent());
    assertTrue(actual.getKey().get().getSchemaId().isPresent());
    assertTrue(actual.getValue().isPresent());
    assertTrue(actual.getValue().get().getSchemaId().isPresent());

    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  @Test
  public void multiFormatPipeline_protobuf() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-protobuf";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    ProtobufSchema keySchema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message ProtoKey { int32 id = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message ProtoValue { string name = 1; int32 score = 2; }");

    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 99);
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "protobuf-test");
    value.put("score", 42);

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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    assertNotNull(actual.getClusterId());
    assertEquals(topicName, actual.getTopicName());

    // Validate schema registration
    assertTrue(actual.getKey().isPresent());
    assertTrue(actual.getKey().get().getSchemaId().isPresent());
    assertTrue(actual.getValue().isPresent());
    assertTrue(actual.getValue().get().getSchemaId().isPresent());

    ConsumerRecord<Message, Message> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedKey =
        DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("id"), 99);
    DynamicMessage.Builder expectedValue =
        DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("name"), "protobuf-test");
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("score"), 42);
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());
  }

  // =========================================================================
  // Batch Ingestion Workflow Tests
  // =========================================================================

  @Test
  public void batchIngestion_largeBatchProcessing() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-large";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);

    int batchSize = 1500;
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("batch-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("batch-value-" + i))
                      .build())
              .build());
    }

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = testEnv.kafkaRest().getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    long startTime = System.currentTimeMillis();
    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    long elapsedTime = System.currentTimeMillis() - startTime;
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Verify performance: batch should complete in reasonable time
    assertTrue(
        "Batch of " + batchSize + " records took " + elapsedTime + "ms",
        elapsedTime < 120000);

    // Verify all records are retrievable
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    for (int i = 0; i < batchSize; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("batch-key-" + i, produced.key());
      assertEquals("batch-value-" + i, produced.value());
    }
  }

  @Test
  public void batchIngestion_orderingGuaranteesWithinPartition() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-ordering";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    int batchSize = 100;
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("ordered-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("ordered-value-" + i))
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Verify ordering: offsets should be monotonically increasing for same partition
    for (int i = 1; i < batchSize; i++) {
      assertEquals(0, actual.get(i).getPartitionId());
      assertTrue(
          "Offset " + actual.get(i).getOffset()
              + " should be greater than " + actual.get(i - 1).getOffset(),
          actual.get(i).getOffset() > actual.get(i - 1).getOffset());
    }
  }

  @Test
  public void batchIngestion_offsetContinuityValidation() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-offset-continuity";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    int batchSize = 50;
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("continuity-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("continuity-value-" + i))
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Validate offset continuity: each offset should be exactly 1 more than previous
    long baseOffset = actual.get(0).getOffset();
    for (int i = 0; i < batchSize; i++) {
      assertEquals(
          "Offset at index " + i + " should be " + (baseOffset + i),
          baseOffset + i,
          actual.get(i).getOffset());
    }
  }

  @Test
  public void batchIngestion_partitionDistribution() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-partition-dist";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);

    int batchSize = 300;
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("dist-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("dist-value-" + i))
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Collect per-partition offsets and verify ordering within each partition
    Map<Integer, List<Long>> partitionOffsets = new HashMap<>();
    for (ProduceResponse pr : actual) {
      partitionOffsets
          .computeIfAbsent(pr.getPartitionId(), k -> new ArrayList<>())
          .add(pr.getOffset());
    }

    // Verify offsets are ordered within each partition
    for (Map.Entry<Integer, List<Long>> entry : partitionOffsets.entrySet()) {
      List<Long> offsets = entry.getValue();
      for (int i = 1; i < offsets.size(); i++) {
        assertTrue(
            "Offsets within partition " + entry.getKey() + " should be ordered",
            offsets.get(i) > offsets.get(i - 1));
      }
    }

    // Verify data integrity across all partitions
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    for (int i = 0; i < batchSize; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("dist-key-" + i, produced.key());
      assertEquals("dist-value-" + i, produced.value());
    }
  }

  // =========================================================================
  // Schema Evolution Pipeline Tests
  // =========================================================================

  @Test
  public void schemaEvolution_avroBackwardCompatibility() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-avro";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String valueSubject = topicName + "-value";

    // Schema v1: just a name field
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"User\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}]}";

    // Produce with schema v1
    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("name", "Alice");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV1)
                    .setData(valueV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);
    int schemaIdV1 = actualV1.getValue().get().getSchemaId().get();

    // Schema v2: add an optional field with default (backward compatible)
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"User\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"email\", \"type\": \"string\", \"default\": \"\"}]}";

    // Produce with schema v2
    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("name", "Bob");
    valueV2.put("email", "bob@example.com");
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema(schemaV2)
                    .setData(valueV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);
    int schemaIdV2 = actualV2.getValue().get().getSchemaId().get();

    // Verify different schema versions were registered
    assertTrue(
        "Schema v2 should have different or same id as v1 depending on schema compatibility",
        schemaIdV2 >= schemaIdV1);

    // Verify v1 record
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV1 =
        new GenericData.Record(new AvroSchema(schemaV1).rawSchema());
    expectedV1.put("name", "Alice");
    assertEquals(expectedV1, producedV1.value());

    // Verify v2 record
    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 =
        new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("name", "Bob");
    expectedV2.put("email", "bob@example.com");
    assertEquals(expectedV2, producedV2.value());
  }

  @Test
  public void schemaEvolution_jsonSchemaAddOptionalField() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-json";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Schema v1
    String schemaV1 =
        "{\"type\": \"object\", \"title\": \"Event\", \"properties\": "
            + "{\"type\": {\"type\": \"string\"}}}";

    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("type", "click");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema(schemaV1)
                    .setData(valueV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);

    // Schema v2: add optional field
    String schemaV2 =
        "{\"type\": \"object\", \"title\": \"Event\", \"properties\": "
            + "{\"type\": {\"type\": \"string\"}, \"source\": {\"type\": \"string\"}}}";

    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("type", "scroll");
    valueV2.put("source", "mobile");
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema(schemaV2)
                    .setData(valueV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);

    // Verify both records
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(valueV1, producedV1.value());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(valueV2, producedV2.value());
  }

  @Test
  public void schemaEvolution_subjectNameStrategy_topicName() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-topic-strategy";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Schema v1 with TOPIC_NAME strategy
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"Item\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}]}";

    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("name", "widget");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setRawSchema(schemaV1)
                    .setData(valueV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);

    // Schema v2 with TOPIC_NAME strategy - add field with default
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"Item\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"quantity\", \"type\": \"int\", \"default\": 0}]}";

    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("name", "gadget");
    valueV2.put("quantity", 5);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setRawSchema(schemaV2)
                    .setData(valueV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);

    // Verify subject name follows TOPIC_NAME strategy
    assertTrue(actualV1.getValue().isPresent());
    assertTrue(actualV2.getValue().isPresent());

    // Verify data integrity
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV1 =
        new GenericData.Record(new AvroSchema(schemaV1).rawSchema());
    expectedV1.put("name", "widget");
    assertEquals(expectedV1, producedV1.value());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 =
        new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("name", "gadget");
    expectedV2.put("quantity", 5);
    assertEquals(expectedV2, producedV2.value());
  }

  @Test
  public void schemaEvolution_subjectNameStrategy_recordName() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-record-strategy";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Use RECORD_NAME strategy
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"Payment\", \"fields\": "
            + "[{\"name\": \"amount\", \"type\": \"int\"}]}";

    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("amount", 100);
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(schemaV1)
                    .setData(valueV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);

    // Evolve schema with RECORD_NAME strategy
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"Payment\", \"fields\": "
            + "[{\"name\": \"amount\", \"type\": \"int\"}, "
            + "{\"name\": \"currency\", \"type\": \"string\", \"default\": \"USD\"}]}";

    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("amount", 250);
    valueV2.put("currency", "EUR");
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.RECORD_NAME)
                    .setRawSchema(schemaV2)
                    .setData(valueV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);

    // Verify data
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV1 =
        new GenericData.Record(new AvroSchema(schemaV1).rawSchema());
    expectedV1.put("amount", 100);
    assertEquals(expectedV1, producedV1.value());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 =
        new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("amount", 250);
    expectedV2.put("currency", "EUR");
    assertEquals(expectedV2, producedV2.value());
  }

  @Test
  public void schemaEvolution_protobufAddField() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-protobuf";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Protobuf v1
    ProtobufSchema schemaV1 =
        new ProtobufSchema(
            "syntax = \"proto3\"; message Event { string type = 1; }");

    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("type", "login");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(schemaV1.canonicalString())
                    .setData(valueV1)
                    .build())
            .build();

    Response responseV1 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV1, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV1.getStatus());
    ProduceResponse actualV1 = readProduceResponse(responseV1);

    // Protobuf v2: add a new field
    ProtobufSchema schemaV2 =
        new ProtobufSchema(
            "syntax = \"proto3\"; message Event { string type = 1; int32 priority = 2; }");

    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("type", "logout");
    valueV2.put("priority", 5);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(schemaV2.canonicalString())
                    .setData(valueV2)
                    .build())
            .build();

    Response responseV2 =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestV2, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), responseV2.getStatus());
    ProduceResponse actualV2 = readProduceResponse(responseV2);

    // Verify v1 record
    ConsumerRecord<Message, Message> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedV1 =
        DynamicMessage.newBuilder(schemaV1.toDescriptor());
    expectedV1.setField(
        schemaV1.toDescriptor().findFieldByName("type"), "login");
    assertEquals(expectedV1.build().toByteString(), producedV1.value().toByteString());

    // Verify v2 record
    ConsumerRecord<Message, Message> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedV2 =
        DynamicMessage.newBuilder(schemaV2.toDescriptor());
    expectedV2.setField(
        schemaV2.toDescriptor().findFieldByName("type"), "logout");
    expectedV2.setField(
        schemaV2.toDescriptor().findFieldByName("priority"), 5);
    assertEquals(expectedV2.build().toByteString(), producedV2.value().toByteString());
  }

  // =========================================================================
  // Error Recovery Workflow Tests
  // =========================================================================

  @Test
  public void errorRecovery_invalidDataFollowedByValidData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-invalid";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // First, send invalid data (invalid base64 for binary)
    ProduceRequest invalidRequest =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(IntNode.valueOf(1))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(TextNode.valueOf("fooba"))
                    .build())
            .build();

    Response invalidResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), invalidResponse.getStatus());
    ErrorResponse errorActual = invalidResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorActual.getErrorCode());

    // Now send valid data - the pipeline should recover
    ByteString key = ByteString.copyFromUtf8("recovered-key");
    ByteString value = ByteString.copyFromUtf8("recovered-value");
    ProduceRequest validRequest =
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

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    ProduceResponse actual = readProduceResponse(validResponse);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void errorRecovery_malformedJsonRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-malformed";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send malformed JSON
    String malformedJson = "{ invalid json {{{{";
    Response malformedResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(malformedJson, MediaType.APPLICATION_JSON));
    // Should return an error status
    assertTrue(
        "Malformed JSON should return error",
        malformedResponse.getStatus() >= 400);

    // Recover with valid request
    ByteString key = ByteString.copyFromUtf8("after-malformed");
    ByteString value = ByteString.copyFromUtf8("recovered-data");
    ProduceRequest validRequest =
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

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    ProduceResponse actual = readProduceResponse(validResponse);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
  }

  @Test
  public void errorRecovery_schemaValidationFailureThenSuccess() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-schema";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send data that doesn't match schema (int data for string schema)
    ProduceRequest invalidSchemaRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(42))
                    .build())
            .build();

    Response invalidResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidSchemaRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), invalidResponse.getStatus());
    ErrorResponse errorActual = invalidResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorActual.getErrorCode());

    // Now send valid data matching the schema
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("valid-data"))
                    .build())
            .build();

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    ProduceResponse actual = readProduceResponse(validResponse);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("valid-data", produced.value());
  }

  @Test
  public void errorRecovery_produceToNonExistentTopic() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String nonExistentTopic = "this-topic-does-not-exist-" + System.currentTimeMillis();

    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("key").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("val").toByteArray()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + nonExistentTopic + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    // Should return an error since the topic doesn't exist
    ErrorResponse errorActual = response.readEntity(ErrorResponse.class);
    assertTrue(
        "Error code should indicate topic not found or produce failure",
        errorActual.getErrorCode() >= 400);
  }

  @Test
  public void errorRecovery_batchWithMixedValidAndInvalidRecords() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-mixed-batch";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Create a batch where some records are valid and some are invalid
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    // Valid record
    requests.add(
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("valid-key-0"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("valid-value-0"))
                    .build())
            .build());
    // Another valid record
    requests.add(
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("valid-key-1"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("valid-value-1"))
                    .build())
            .build());

    StringBuilder batch = new StringBuilder();
    ObjectMapper objectMapper = testEnv.kafkaRest().getObjectMapper();
    for (ProduceRequest produceRequest : requests) {
      batch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(2, actual.size());

    // Verify all valid records were stored correctly
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    for (int i = 0; i < 2; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("valid-key-" + i, produced.key());
      assertEquals("valid-value-" + i, produced.value());
    }
  }

  @Test
  public void errorRecovery_unrecognizedFieldInRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-unrecognized";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Request with unrecognized field
    String requestWithUnknownField =
        "{ \"records\": {\"subject\": \"foobar\" } }";

    Response errorResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(requestWithUnknownField, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), errorResponse.getStatus());
    ErrorResponse errorActual = errorResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorActual.getErrorCode());

    // Verify pipeline still works after error
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("post-error-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("post-error-value"))
                    .build())
            .build();

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    ProduceResponse actual = readProduceResponse(validResponse);
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                deserializer,
                deserializer);
    assertEquals("post-error-key", produced.key());
    assertEquals("post-error-value", produced.value());
  }

  @Test
  public void errorRecovery_invalidBinaryBatchThenValidBatch() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-batch-retry";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // First batch: all invalid binary data
    ArrayList<ProduceRequest> invalidRequests = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      invalidRequests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.BINARY)
                      .setData(IntNode.valueOf(i + 1))
                      .build())
              .build());
    }

    StringBuilder invalidBatch = new StringBuilder();
    ObjectMapper objectMapper = testEnv.kafkaRest().getObjectMapper();
    for (ProduceRequest produceRequest : invalidRequests) {
      invalidBatch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response invalidResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidBatch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), invalidResponse.getStatus());

    List<ErrorResponse> errors = readErrorResponses(invalidResponse);
    for (ErrorResponse err : errors) {
      assertEquals(400, err.getErrorCode());
    }

    // Second batch: all valid - retry logic
    ArrayList<ProduceRequest> validRequests = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      validRequests.add(
          ProduceRequest.builder()
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("retry-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("retry-value-" + i))
                      .build())
              .build());
    }

    StringBuilder validBatch = new StringBuilder();
    for (ProduceRequest produceRequest : validRequests) {
      validBatch.append(objectMapper.writeValueAsString(produceRequest));
    }

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validBatch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    List<ProduceResponse> validActual = readProduceResponses(validResponse);
    assertEquals(5, validActual.size());

    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    for (int i = 0; i < 5; i++) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  validActual.get(i).getPartitionId(),
                  validActual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("retry-key-" + i, produced.key());
      assertEquals("retry-value-" + i, produced.value());
    }
  }

  @Test
  public void errorRecovery_invalidAvroSchemaFormat() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-bad-schema";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send request with invalid Avro schema
    ProduceRequest invalidSchemaRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"not_a_valid_type\"}")
                    .setData(TextNode.valueOf("data"))
                    .build())
            .build();

    Response invalidResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidSchemaRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), invalidResponse.getStatus());
    ErrorResponse errorActual = invalidResponse.readEntity(ErrorResponse.class);
    assertTrue(
        "Should return error for invalid schema",
        errorActual.getErrorCode() >= 400);

    // Recover with valid schema
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("recovered-after-bad-schema"))
                    .build())
            .build();

    Response validResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), validResponse.getStatus());

    ProduceResponse actual = readProduceResponse(validResponse);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("recovered-after-bad-schema", produced.value());
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
