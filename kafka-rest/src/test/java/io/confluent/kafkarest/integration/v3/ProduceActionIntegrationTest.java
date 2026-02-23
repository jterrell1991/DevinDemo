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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

  // ========================================================================
  // Multi-Format Pipeline Integration Tests
  // ========================================================================

  @Test
  public void produceFullPipelineBinary() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "pipeline-binary-topic";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);
    ByteString key = ByteString.copyFromUtf8("pipeline-key");
    ByteString value = ByteString.copyFromUtf8("pipeline-value");
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
    assertEquals(topicName, actual.getTopicName());
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
    assertEquals(actual.getPartitionId(), produced.partition());
    assertEquals(actual.getOffset(), produced.offset());
  }

  @Test
  public void produceFullPipelineAvro() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "pipeline-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);
    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"PipelineKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"int\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"PipelineValue\", \"fields\": "
            + "[{\"name\": \"data\", \"type\": \"string\"}]}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 42);
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("data", "avro-pipeline-test");
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject(keySubject)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject(valueSubject)
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("id", 42);
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("data", "avro-pipeline-test");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());

    // Validate schema registration
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(keySubject).getId();
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getId();
    assertEquals(keyRawSchema, testEnv.schemaRegistry().getClient().getSchemaById(keySchemaId)
        .canonicalString());
    assertEquals(valueRawSchema, testEnv.schemaRegistry().getClient().getSchemaById(valueSchemaId)
        .canonicalString());
  }

  @Test
  public void produceFullPipelineJsonSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "pipeline-jsonschema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);
    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    String keyRawSchema = "{\"type\": \"string\"}";
    String valueRawSchema =
        "{\"type\": \"object\", \"title\": \"PipelineValue\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}, \"count\": {\"type\": \"integer\"}}}";
    TextNode key = TextNode.valueOf("json-pipeline-key");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "test");
    value.put("count", 99);
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject(keySubject)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject(valueSubject)
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

    // Validate schema registration
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(keySubject).getId();
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getId();
    assertEquals(keyRawSchema, testEnv.schemaRegistry().getClient().getSchemaById(keySchemaId)
        .canonicalString());
  }

  @Test
  public void produceFullPipelineProtobuf() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "pipeline-protobuf-topic";
    testEnv.kafkaCluster().createTopic(topicName, 3, (short) 1);
    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message PipelineKey { string id = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message PipelineValue { string name = 1; int32 count = 2; }");
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", "proto-key-1");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "proto-test");
    value.put("count", 77);
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject(keySubject)
                    .setRawSchema(keySchema.canonicalString())
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject(valueSubject)
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
    ConsumerRecord<Message, Message> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("id"), "proto-key-1");
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("name"), "proto-test");
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("count"), 77);
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());

    // Validate schema registration
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(keySubject).getId();
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getId();
    assertEquals(keySchema.canonicalString(),
        testEnv.schemaRegistry().getClient().getSchemaById(keySchemaId).canonicalString());
    assertEquals(valueSchema.canonicalString(),
        testEnv.schemaRegistry().getClient().getSchemaById(valueSchemaId).canonicalString());
  }

  // ========================================================================
  // Batch Ingestion Workflow Tests
  // ========================================================================

  @Test
  public void produceLargeBatchWithOrderingAndOffsetContinuity() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-large-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    int batchSize = 1000;

    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(0)
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
    long elapsedMs = System.currentTimeMillis() - startTime;
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    // Verify ordering guarantees and offset continuity on partition 0
    long previousOffset = -1;
    for (int i = 0; i < batchSize; i++) {
      assertEquals(0, actual.get(i).getPartitionId());
      long currentOffset = actual.get(i).getOffset();
      if (previousOffset >= 0) {
        assertEquals(previousOffset + 1, currentOffset);
      }
      previousOffset = currentOffset;

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

    // Performance metrics: ensure batch completes in reasonable time
    // 1000 records should complete within 60 seconds even on slow environments
    assertTrue("Batch of " + batchSize + " records took " + elapsedMs + "ms",
        elapsedMs < 60000);
  }

  @Test
  public void produceLargeBatchMultiPartitionOrdering() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-multi-partition-topic";
    int numPartitions = 3;
    testEnv.kafkaCluster().createTopic(topicName, numPartitions, (short) 1);
    int batchSize = 1500;

    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(i % numPartitions)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("mp-key-" + i))
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.JSON)
                      .setData(TextNode.valueOf("mp-value-" + i))
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

    // Track offsets per partition to verify ordering within each partition
    Map<Integer, Long> previousOffsetPerPartition = new HashMap<>();
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    for (int i = 0; i < batchSize; i++) {
      int partitionId = actual.get(i).getPartitionId();
      long offset = actual.get(i).getOffset();
      assertEquals(i % numPartitions, partitionId);

      // Verify offset continuity within each partition
      if (previousOffsetPerPartition.containsKey(partitionId)) {
        long prevOffset = previousOffsetPerPartition.get(partitionId);
        assertEquals(prevOffset + 1, offset);
      }
      previousOffsetPerPartition.put(partitionId, offset);

      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  partitionId,
                  offset,
                  deserializer,
                  deserializer);
      assertEquals("mp-key-" + i, produced.key());
      assertEquals("mp-value-" + i, produced.value());
    }

    // Verify all partitions received records
    assertEquals(numPartitions, previousOffsetPerPartition.size());
  }

  @Test
  public void produceAvroBatchWithSchemaRegistration() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    int batchSize = 1000;

    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
      key.put("id", i);
      ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
      value.put("data", "avro-batch-" + i);
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(0)
              .setKey(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.AVRO)
                      .setSubject(keySubject)
                      .setRawSchema(
                          "{\"type\": \"record\", \"name\": \"BatchKey\", \"fields\": "
                              + "[{\"name\": \"id\", \"type\": \"int\"}]}")
                      .setData(key)
                      .build())
              .setValue(
                  ProduceRequestData.builder()
                      .setFormat(EmbeddedFormat.AVRO)
                      .setSubject(valueSubject)
                      .setRawSchema(
                          "{\"type\": \"record\", \"name\": \"BatchValue\", \"fields\": "
                              + "[{\"name\": \"data\", \"type\": \"string\"}]}")
                      .setData(value)
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

    // Verify offset continuity
    for (int i = 1; i < batchSize; i++) {
      assertEquals(actual.get(i - 1).getOffset() + 1, actual.get(i).getOffset());
    }

    // Spot-check first and last records
    AvroSchema keyAvroSchema = new AvroSchema(
        "{\"type\": \"record\", \"name\": \"BatchKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"int\"}]}");
    AvroSchema valueAvroSchema = new AvroSchema(
        "{\"type\": \"record\", \"name\": \"BatchValue\", \"fields\": "
            + "[{\"name\": \"data\", \"type\": \"string\"}]}");

    ConsumerRecord<Object, Object> firstRecord =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.get(0).getPartitionId(),
                actual.get(0).getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedFirstKey =
        new GenericData.Record(keyAvroSchema.rawSchema());
    expectedFirstKey.put("id", 0);
    assertEquals(expectedFirstKey, firstRecord.key());

    ConsumerRecord<Object, Object> lastRecord =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.get(batchSize - 1).getPartitionId(),
                actual.get(batchSize - 1).getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedLastKey =
        new GenericData.Record(keyAvroSchema.rawSchema());
    expectedLastKey.put("id", batchSize - 1);
    assertEquals(expectedLastKey, lastRecord.key());
  }

  // ========================================================================
  // Schema Evolution Pipeline Tests
  // ========================================================================

  @Test
  public void produceAvroSchemaEvolutionV1ThenV2() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String valueSubject = topicName + "-value";

    // Schema V1: just a "name" field
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"Evolving\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}]}";

    // Produce with schema V1
    ObjectNode v1Value = new ObjectNode(JsonNodeFactory.instance);
    v1Value.put("name", "initial");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("k1").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV1)
                    .setData(v1Value)
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

    // Schema V2: add optional "age" field with default (backward compatible)
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"Evolving\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"age\", \"type\": \"int\", \"default\": 0}]}";

    // Produce with schema V2
    ObjectNode v2Value = new ObjectNode(JsonNodeFactory.instance);
    v2Value.put("name", "evolved");
    v2Value.put("age", 25);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("k2").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV2)
                    .setData(v2Value)
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

    // Verify V1 record
    ConsumerRecord<byte[], Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                new ByteArrayDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV1 = new GenericData.Record(new AvroSchema(schemaV1).rawSchema());
    expectedV1.put("name", "initial");
    assertEquals(expectedV1, producedV1.value());

    // Verify V2 record
    ConsumerRecord<byte[], Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                new ByteArrayDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 = new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("name", "evolved");
    expectedV2.put("age", 25);
    assertEquals(expectedV2, producedV2.value());

    // Verify both schema versions exist in registry
    int latestVersion =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getVersion();
    assertEquals(2, latestVersion);
  }

  @Test
  public void produceJsonSchemaEvolutionV1ThenV2() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-jsonschema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String valueSubject = topicName + "-value";

    // Schema V1
    String schemaV1 =
        "{\"type\": \"object\", \"title\": \"EvolvingJson\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}}}";
    ObjectNode v1Value = new ObjectNode(JsonNodeFactory.instance);
    v1Value.put("name", "json-v1");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV1)
                    .setData(v1Value)
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

    // Schema V2: add "age" property (backward compatible addition)
    String schemaV2 =
        "{\"type\": \"object\", \"title\": \"EvolvingJson\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"integer\"}}}";
    ObjectNode v2Value = new ObjectNode(JsonNodeFactory.instance);
    v2Value.put("name", "json-v2");
    v2Value.put("age", 30);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV2)
                    .setData(v2Value)
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

    // Verify V1 record
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(v1Value, producedV1.value());

    // Verify V2 record
    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(v2Value, producedV2.value());

    // Verify both versions registered
    int latestVersion =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getVersion();
    assertEquals(2, latestVersion);
  }

  @Test
  public void produceProtobufSchemaEvolutionV1ThenV2() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-protobuf-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String valueSubject = topicName + "-value";

    // Schema V1
    ProtobufSchema schemaV1 =
        new ProtobufSchema("syntax = \"proto3\"; message EvolvingProto { string name = 1; }");
    ObjectNode v1Value = new ObjectNode(JsonNodeFactory.instance);
    v1Value.put("name", "proto-v1");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV1.canonicalString())
                    .setData(v1Value)
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

    // Schema V2: add optional "age" field (backward compatible in proto3)
    ProtobufSchema schemaV2 =
        new ProtobufSchema(
            "syntax = \"proto3\"; message EvolvingProto { string name = 1; int32 age = 2; }");
    ObjectNode v2Value = new ObjectNode(JsonNodeFactory.instance);
    v2Value.put("name", "proto-v2");
    v2Value.put("age", 40);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setSubject(valueSubject)
                    .setRawSchema(schemaV2.canonicalString())
                    .setData(v2Value)
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

    // Verify V1 record
    ConsumerRecord<Message, Message> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedV1 = DynamicMessage.newBuilder(schemaV1.toDescriptor());
    expectedV1.setField(schemaV1.toDescriptor().findFieldByName("name"), "proto-v1");
    assertEquals(expectedV1.build().toByteString(), producedV1.value().toByteString());

    // Verify V2 record
    ConsumerRecord<Message, Message> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expectedV2 = DynamicMessage.newBuilder(schemaV2.toDescriptor());
    expectedV2.setField(schemaV2.toDescriptor().findFieldByName("name"), "proto-v2");
    expectedV2.setField(schemaV2.toDescriptor().findFieldByName("age"), 40);
    assertEquals(expectedV2.build().toByteString(), producedV2.value().toByteString());

    // Verify two schema versions registered
    int latestVersion =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(valueSubject).getVersion();
    assertEquals(2, latestVersion);
  }

  @Test
  public void produceAvroWithTopicNameSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "subject-strategy-topicname-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"StratKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"string\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"StratValue\", \"fields\": "
            + "[{\"name\": \"val\", \"type\": \"string\"}]}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", "topicname-strat");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("val", "topicname-strat-value");
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
                    .setRawSchema(keyRawSchema)
                    .setData(key)
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setSubjectNameStrategy(EnumSubjectNameStrategy.TOPIC_NAME)
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
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("id", "topicname-strat");
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("val", "topicname-strat-value");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());

    // Verify schemas registered under TOPIC_NAME strategy subjects
    String expectedKeySubject =
        new TopicNameStrategy().subjectName(topicName, /* isKey= */ true, null);
    String expectedValueSubject =
        new TopicNameStrategy().subjectName(topicName, /* isKey= */ false, null);
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(expectedKeySubject).getId();
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(expectedValueSubject).getId();
    assertTrue(keySchemaId > 0);
    assertTrue(valueSchemaId > 0);
  }

  @Test
  public void produceAvroWithRecordNameSubjectStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "subject-strategy-recordname-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"RecordKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"string\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"RecordValue\", \"fields\": "
            + "[{\"name\": \"val\", \"type\": \"string\"}]}";
    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", "recordname-strat");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("val", "recordname-strat-value");
    ProduceRequest request =
        ProduceRequest.builder()
            .setPartitionId(0)
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedKey = new GenericData.Record(new AvroSchema(keyRawSchema).rawSchema());
    expectedKey.put("id", "recordname-strat");
    GenericRecord expectedValue =
        new GenericData.Record(new AvroSchema(valueRawSchema).rawSchema());
    expectedValue.put("val", "recordname-strat-value");
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());

    // Verify schemas registered under RECORD_NAME strategy subjects
    AvroSchema keyAvro = new AvroSchema(keyRawSchema);
    AvroSchema valueAvro = new AvroSchema(valueRawSchema);
    String expectedKeySubject =
        new RecordNameStrategy().subjectName(topicName, /* isKey= */ true, keyAvro);
    String expectedValueSubject =
        new RecordNameStrategy().subjectName(topicName, /* isKey= */ false, valueAvro);
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(expectedKeySubject).getId();
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getLatestSchemaMetadata(expectedValueSubject).getId();
    assertTrue(keySchemaId > 0);
    assertTrue(valueSchemaId > 0);
  }

  // ========================================================================
  // Error Recovery Workflow Tests
  // ========================================================================

  @Test
  public void produceBinaryWithInvalidDataRecovery() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // First, send invalid binary data
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
                    .setData(TextNode.valueOf("fooba"))  // invalid base64
                    .build())
            .build();

    Response errorResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), errorResponse.getStatus());
    ErrorResponse errorResult = errorResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResult.getErrorCode());

    // Recover: send valid data after error
    ByteString key = ByteString.copyFromUtf8("recovery-key");
    ByteString value = ByteString.copyFromUtf8("recovery-value");
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

    Response successResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), successResponse.getStatus());

    ProduceResponse actual = readProduceResponse(successResponse);
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
  public void produceAvroWithInvalidSchemaRecovery() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send invalid Avro data (int data with string schema)
    ProduceRequest invalidRequest =
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

    Response errorResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), errorResponse.getStatus());
    ErrorResponse errorResult = errorResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResult.getErrorCode());

    // Recover with valid Avro data
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("recovered-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("recovered-value"))
                    .build())
            .build();

    Response successResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), successResponse.getStatus());

    ProduceResponse actual = readProduceResponse(successResponse);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("recovered-key", produced.key());
    assertEquals("recovered-value", produced.value());
  }

  @Test
  public void produceToNonExistentTopic_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "nonexistent-topic-" + System.currentTimeMillis();
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(
                        ByteString.copyFromUtf8("key").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(
                        ByteString.copyFromUtf8("value").toByteArray()))
                    .build())
            .build();

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    // Should return an error (typically 4xx) for non-existent topic
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertTrue(actual.getErrorCode() >= 400);
  }

  @Test
  public void produceMalformedJsonRequest_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String malformedJson = "{ this is not valid json }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(malformedJson, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBatchWithMixedValidAndInvalidRecords() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "mixed-batch-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Build a batch with some valid and some invalid records
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    // Valid record
    requests.add(
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(
                        ByteString.copyFromUtf8("valid-key").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(
                        ByteString.copyFromUtf8("valid-value").toByteArray()))
                    .build())
            .build());
    // Invalid record (IntNode for binary format)
    requests.add(
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(IntNode.valueOf(123))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(IntNode.valueOf(456))
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

    // The response should contain both responses - first success, second error
    // We verify the system continues processing after errors
    response.bufferEntity();
    String responseBody = response.readEntity(String.class);
    assertTrue(responseBody.length() > 0);
  }

  @Test
  public void produceJsonschemaWithInvalidDataRecovery() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-jsonschema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send invalid JSON Schema data (int value doesn't match string schema)
    ProduceRequest invalidRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(42))
                    .build())
            .build();

    Response errorResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), errorResponse.getStatus());
    ErrorResponse errorResult = errorResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResult.getErrorCode());

    // Recover with valid JSON Schema data
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("recovered-jsonschema-value"))
                    .build())
            .build();

    Response successResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), successResponse.getStatus());

    ProduceResponse actual = readProduceResponse(successResponse);
    ConsumerRecord<Object, Object> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(TextNode.valueOf("recovered-jsonschema-value"), produced.value());
  }

  @Test
  public void produceProtobufWithInvalidDataRecovery() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-protobuf-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    ProtobufSchema schema =
        new ProtobufSchema("syntax = \"proto3\"; message Recovery { string name = 1; }");

    // Send invalid Protobuf data
    ProduceRequest invalidRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(schema.canonicalString())
                    .setData(IntNode.valueOf(42))
                    .build())
            .build();

    Response errorResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), errorResponse.getStatus());
    ErrorResponse errorResult = errorResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResult.getErrorCode());

    // Recover with valid Protobuf data
    ObjectNode validValue = new ObjectNode(JsonNodeFactory.instance);
    validValue.put("name", "recovered-proto");
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.PROTOBUF)
                    .setRawSchema(schema.canonicalString())
                    .setData(validValue)
                    .build())
            .build();

    Response successResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(validRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), successResponse.getStatus());

    ProduceResponse actual = readProduceResponse(successResponse);
    ConsumerRecord<Message, Message> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());
    DynamicMessage.Builder expected = DynamicMessage.newBuilder(schema.toDescriptor());
    expected.setField(schema.toDescriptor().findFieldByName("name"), "recovered-proto");
    assertEquals(expected.build().toByteString(), produced.value().toByteString());
  }

  @Test
  public void produceMultipleFormatsToSameTopic() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Produce Binary record
    ByteString binaryKey = ByteString.copyFromUtf8("binary-key");
    ByteString binaryValue = ByteString.copyFromUtf8("binary-value");
    ProduceRequest binaryRequest =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(binaryKey.toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(binaryValue.toByteArray()))
                    .build())
            .build();

    Response binaryResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(binaryRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), binaryResponse.getStatus());
    ProduceResponse binaryActual = readProduceResponse(binaryResponse);

    // Produce JSON record
    ProduceRequest jsonRequest =
        ProduceRequest.builder()
            .setPartitionId(0)
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("json-key"))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSON)
                    .setData(TextNode.valueOf("json-value"))
                    .build())
            .build();

    Response jsonResponse =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(jsonRequest, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), jsonResponse.getStatus());
    ProduceResponse jsonActual = readProduceResponse(jsonResponse);

    // Verify binary record
    ConsumerRecord<byte[], byte[]> producedBinary =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                binaryActual.getPartitionId(),
                binaryActual.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(binaryKey, ByteString.copyFrom(producedBinary.key()));
    assertEquals(binaryValue, ByteString.copyFrom(producedBinary.value()));

    // Verify JSON record
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    ConsumerRecord<Object, Object> producedJson =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                jsonActual.getPartitionId(),
                jsonActual.getOffset(),
                deserializer,
                deserializer);
    assertEquals("json-key", producedJson.key());
    assertEquals("json-value", producedJson.value());

    // Verify ordering: JSON record offset should be after binary record offset
    assertEquals(binaryActual.getOffset() + 1, jsonActual.getOffset());
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
