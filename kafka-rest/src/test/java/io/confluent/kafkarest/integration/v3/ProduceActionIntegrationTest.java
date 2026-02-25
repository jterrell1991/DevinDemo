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

  // ==================== Multi-Format Pipeline Integration Tests ====================

  @Test
  public void produceMultiFormatPipeline_binary() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-binary-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

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
    assertEquals(0, actual.getPartitionId());
    assertEquals(0, actual.getOffset());

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
    assertEquals(topicName, produced.topic());
  }

  @Test
  public void produceMultiFormatPipeline_avro() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    String keyRawSchema =
        "{\"type\": \"record\", \"name\": \"PipelineKey\", \"fields\": "
            + "[{\"name\": \"id\", \"type\": \"int\"}]}";
    String valueRawSchema =
        "{\"type\": \"record\", \"name\": \"PipelineValue\", \"fields\": "
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
    expectedValue.put("name", "test-record");
    expectedValue.put("count", 100);
    assertEquals(expectedKey, produced.key());
    assertEquals(expectedValue, produced.value());

    // Verify schema was registered in Schema Registry
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getId(keySubject, new AvroSchema(keyRawSchema));
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getId(valueSubject, new AvroSchema(valueRawSchema));
    // Schema IDs should be positive integers indicating successful registration
    assertTrue("Key schema ID should be positive", keySchemaId > 0);
    assertTrue("Value schema ID should be positive", valueSchemaId > 0);
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(keySubject,
        new AvroSchema(keyRawSchema)));
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(valueSubject,
        new AvroSchema(valueRawSchema)));
  }

  @Test
  public void produceMultiFormatPipeline_jsonSchema() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-jsonschema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    String keyRawSchema = "{\"type\": \"string\"}";
    String valueRawSchema =
        "{\"type\": \"object\", \"title\": \"PipelineValue\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"integer\"}}}";

    TextNode key = TextNode.valueOf("json-key");
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "Alice");
    value.put("age", 30);

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

    // Verify schema was registered
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getId(keySubject, new JsonSchema(keyRawSchema));
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getId(valueSubject, new JsonSchema(valueRawSchema));
    assertTrue("Key schema ID should be positive", keySchemaId > 0);
    assertTrue("Value schema ID should be positive", valueSchemaId > 0);
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(keySubject,
        new JsonSchema(keyRawSchema)));
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(valueSubject,
        new JsonSchema(valueRawSchema)));
  }

  @Test
  public void produceMultiFormatPipeline_protobuf() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "multi-format-protobuf-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String keySubject = topicName + "-key";
    String valueSubject = topicName + "-value";
    ProtobufSchema keySchema =
        new ProtobufSchema("syntax = \"proto3\"; message PipelineKey { int32 id = 1; }");
    ProtobufSchema valueSchema =
        new ProtobufSchema(
            "syntax = \"proto3\"; message PipelineValue { string name = 1; int32 count = 2; }");

    ObjectNode key = new ObjectNode(JsonNodeFactory.instance);
    key.put("id", 42);
    ObjectNode value = new ObjectNode(JsonNodeFactory.instance);
    value.put("name", "test-record");
    value.put("count", 100);

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
    ConsumerRecord<Message, Message> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actual.getPartitionId(),
                actual.getOffset(),
                testEnv.schemaRegistry().createProtobufDeserializer(),
                testEnv.schemaRegistry().createProtobufDeserializer());

    DynamicMessage.Builder expectedKey = DynamicMessage.newBuilder(keySchema.toDescriptor());
    expectedKey.setField(keySchema.toDescriptor().findFieldByName("id"), 42);
    DynamicMessage.Builder expectedValue = DynamicMessage.newBuilder(valueSchema.toDescriptor());
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("name"), "test-record");
    expectedValue.setField(valueSchema.toDescriptor().findFieldByName("count"), 100);
    assertEquals(expectedKey.build().toByteString(), produced.key().toByteString());
    assertEquals(expectedValue.build().toByteString(), produced.value().toByteString());

    // Verify schema was registered
    int keySchemaId =
        testEnv.schemaRegistry().getClient().getId(keySubject, keySchema);
    int valueSchemaId =
        testEnv.schemaRegistry().getClient().getId(valueSubject, valueSchema);
    assertTrue("Key schema ID should be positive", keySchemaId > 0);
    assertTrue("Value schema ID should be positive", valueSchemaId > 0);
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(keySubject, keySchema));
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(valueSubject, valueSchema));
  }

  // ==================== Batch Ingestion Workflow Tests ====================

  @Test
  public void produceLargeBatchWithOrderingAndOffsetContinuity() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-ordering-topic";
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
    long endTime = System.currentTimeMillis();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Verify ordering guarantees: all records should be on partition 0
    for (int i = 0; i < batchSize; i++) {
      assertEquals(0, actual.get(i).getPartitionId());
    }

    // Verify offset continuity: offsets should be sequential
    for (int i = 1; i < batchSize; i++) {
      assertEquals(
          actual.get(i - 1).getOffset() + 1,
          actual.get(i).getOffset());
    }

    // Verify data integrity for sampled records
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);

    // Verify first, middle, and last records
    int[] sampleIndices = {0, batchSize / 2, batchSize - 1};
    for (int i : sampleIndices) {
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

    // Performance metric: batch should complete (no assertion, just ensuring it completes)
    long durationMs = endTime - startTime;
  }

  @Test
  public void produceBatchAcrossPartitions() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "batch-multipartition-topic";
    int numPartitions = 3;
    testEnv.kafkaCluster().createTopic(topicName, numPartitions, (short) 1);

    int batchSize = 300;
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      requests.add(
          ProduceRequest.builder()
              .setPartitionId(i % numPartitions)
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
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    List<ProduceResponse> actual = readProduceResponses(response);
    assertEquals(batchSize, actual.size());

    // Verify records are distributed across partitions
    int[] partitionCounts = new int[numPartitions];
    for (int i = 0; i < batchSize; i++) {
      assertEquals(i % numPartitions, actual.get(i).getPartitionId());
      partitionCounts[actual.get(i).getPartitionId()]++;
    }
    // Each partition should have batchSize/numPartitions records
    for (int count : partitionCounts) {
      assertEquals(batchSize / numPartitions, count);
    }

    // Verify offset continuity within each partition
    long[] lastOffsetPerPartition = new long[numPartitions];
    Arrays.fill(lastOffsetPerPartition, -1);
    for (int i = 0; i < batchSize; i++) {
      int partition = actual.get(i).getPartitionId();
      long offset = actual.get(i).getOffset();
      if (lastOffsetPerPartition[partition] >= 0) {
        assertEquals(lastOffsetPerPartition[partition] + 1, offset);
      }
      lastOffsetPerPartition[partition] = offset;
    }

    // Verify data correctness for sampled records
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(emptyMap(), /* isKey= */ false);
    for (int i = 0; i < batchSize; i += 50) {
      ConsumerRecord<Object, Object> produced =
          testEnv.kafkaCluster()
              .getRecord(
                  topicName,
                  actual.get(i).getPartitionId(),
                  actual.get(i).getOffset(),
                  deserializer,
                  deserializer);
      assertEquals("key-" + i, produced.key());
      assertEquals("value-" + i, produced.value());
    }
  }

  // ==================== Schema Evolution Pipeline Tests ====================

  @Test
  public void produceAvroSchemaEvolution_backwardCompatible() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String valueSubject = topicName + "-value";

    // Schema V1: {name: string}
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"Evolved\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}]}";
    // Schema V2: {name: string, age: int (default 0)} - backward compatible
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"Evolved\", \"fields\": "
            + "[{\"name\": \"name\", \"type\": \"string\"}, "
            + "{\"name\": \"age\", \"type\": \"int\", \"default\": 0}]}";

    // Produce with schema V1
    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("name", "Alice");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
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

    // Verify V1 schema registered
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(valueSubject,
        new AvroSchema(schemaV1)));

    // Produce with schema V2 (evolved)
    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("name", "Bob");
    valueV2.put("age", 25);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
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

    // Verify V2 schema registered as new version
    assertEquals(2, testEnv.schemaRegistry().getClient().getVersion(valueSubject,
        new AvroSchema(schemaV2)));

    // Verify both records can be read back
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV1 = new GenericData.Record(new AvroSchema(schemaV1).rawSchema());
    expectedV1.put("name", "Alice");
    assertEquals(expectedV1.get("name").toString(), ((GenericRecord) producedV1.value()).get("name").toString());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 = new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("name", "Bob");
    expectedV2.put("age", 25);
    assertEquals(expectedV2, producedV2.value());
  }

  @Test
  public void produceJsonSchemaEvolution_backwardCompatible() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-json-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    String valueSubject = topicName + "-value";

    // Schema V1
    String schemaV1 =
        "{\"type\": \"object\", \"title\": \"EvolvedJson\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}}}";
    // Schema V2: adds optional property (backward compatible)
    String schemaV2 =
        "{\"type\": \"object\", \"title\": \"EvolvedJson\", \"properties\": "
            + "{\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"integer\"}}}";

    // Produce with V1
    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("name", "Alice");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
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

    // Produce with V2
    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("name", "Bob");
    valueV2.put("age", 25);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
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

    // Verify both records retrievable
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
  public void produceSchemaEvolution_topicNameStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-topicname-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Schema V1 with TOPIC_NAME strategy
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"TopicEvolved\", \"fields\": "
            + "[{\"name\": \"field1\", \"type\": \"string\"}]}";
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"TopicEvolved\", \"fields\": "
            + "[{\"name\": \"field1\", \"type\": \"string\"}, "
            + "{\"name\": \"field2\", \"type\": \"int\", \"default\": 0}]}";

    // Produce with V1 using TOPIC_NAME strategy
    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("field1", "hello");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
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

    // Verify schema registered under topic-name subject
    String expectedSubject =
        new TopicNameStrategy().subjectName(topicName, /* isKey= */ false,
            new AvroSchema(schemaV1));
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(expectedSubject,
        new AvroSchema(schemaV1)));

    // Produce with V2 using TOPIC_NAME strategy
    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("field1", "world");
    valueV2.put("field2", 42);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
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

    // Verify V2 registered under same subject
    assertEquals(2, testEnv.schemaRegistry().getClient().getVersion(expectedSubject,
        new AvroSchema(schemaV2)));

    // Verify both records
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("hello",
        ((GenericRecord) producedV1.value()).get("field1").toString());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 = new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("field1", "world");
    expectedV2.put("field2", 42);
    assertEquals(expectedV2, producedV2.value());
  }

  @Test
  public void produceSchemaEvolution_recordNameStrategy() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "schema-evolution-recordname-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Schema V1 with RECORD_NAME strategy
    String schemaV1 =
        "{\"type\": \"record\", \"name\": \"RecordEvolved\", \"fields\": "
            + "[{\"name\": \"data\", \"type\": \"string\"}]}";
    String schemaV2 =
        "{\"type\": \"record\", \"name\": \"RecordEvolved\", \"fields\": "
            + "[{\"name\": \"data\", \"type\": \"string\"}, "
            + "{\"name\": \"version\", \"type\": \"int\", \"default\": 1}]}";

    // Produce with V1 using RECORD_NAME strategy
    ObjectNode valueV1 = new ObjectNode(JsonNodeFactory.instance);
    valueV1.put("data", "first");
    ProduceRequest requestV1 =
        ProduceRequest.builder()
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

    // Verify schema registered under record-name subject
    String expectedSubject =
        new RecordNameStrategy().subjectName(topicName, /* isKey= */ false,
            new AvroSchema(schemaV1));
    assertEquals(1, testEnv.schemaRegistry().getClient().getVersion(expectedSubject,
        new AvroSchema(schemaV1)));

    // Produce with V2 using RECORD_NAME strategy
    ObjectNode valueV2 = new ObjectNode(JsonNodeFactory.instance);
    valueV2.put("data", "second");
    valueV2.put("version", 2);
    ProduceRequest requestV2 =
        ProduceRequest.builder()
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

    // Verify V2 registered under same record-name subject
    assertEquals(2, testEnv.schemaRegistry().getClient().getVersion(expectedSubject,
        new AvroSchema(schemaV2)));

    // Verify both records
    ConsumerRecord<Object, Object> producedV1 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV1.getPartitionId(),
                actualV1.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    assertEquals("first",
        ((GenericRecord) producedV1.value()).get("data").toString());

    ConsumerRecord<Object, Object> producedV2 =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                actualV2.getPartitionId(),
                actualV2.getOffset(),
                testEnv.schemaRegistry().createAvroDeserializer(),
                testEnv.schemaRegistry().createAvroDeserializer());
    GenericRecord expectedV2 = new GenericData.Record(new AvroSchema(schemaV2).rawSchema());
    expectedV2.put("data", "second");
    expectedV2.put("version", 2);
    assertEquals(expectedV2, producedV2.value());
  }

  // ==================== Error Recovery Workflow Tests ====================

  @Test
  public void produceInvalidBinaryData_recoversWithValidData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-binary-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // First, send invalid data
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
                    .setData(TextNode.valueOf("fooba")) // invalid base64
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
    ErrorResponse errorResponse = invalidResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResponse.getErrorCode());

    // Now send valid data to verify recovery
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
  public void produceInvalidAvroData_recoversWithValidData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-avro-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send invalid data (int data for string schema)
    ProduceRequest invalidRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(999))
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
    ErrorResponse errorResponse = invalidResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResponse.getErrorCode());

    // Recover with valid data
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf("recovered"))
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
    assertEquals("recovered", produced.value());
  }

  @Test
  public void produceToNonExistentTopic_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String nonExistentTopic = "non-existent-topic-xyz";

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
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("value").toByteArray()))
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

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceMalformedJsonRequest_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-malformed-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send malformed JSON
    String malformedRequest = "{ invalid json content }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(malformedRequest, MediaType.APPLICATION_JSON));
    // Should return an error status
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());
  }

  @Test
  public void produceBatchWithMixedValidAndInvalidData_returnsPartialErrors() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-batch-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Build a batch with alternating valid and invalid binary requests
    ArrayList<ProduceRequest> requests = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        // Valid binary request
        requests.add(
            ProduceRequest.builder()
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.BINARY)
                        .setData(BinaryNode.valueOf(
                            ByteString.copyFromUtf8("key-" + i).toByteArray()))
                        .build())
                .setValue(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.BINARY)
                        .setData(BinaryNode.valueOf(
                            ByteString.copyFromUtf8("value-" + i).toByteArray()))
                        .build())
                .build());
      } else {
        // Invalid binary request (int data for binary format)
        requests.add(
            ProduceRequest.builder()
                .setKey(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.BINARY)
                        .setData(IntNode.valueOf(i))
                        .build())
                .setValue(
                    ProduceRequestData.builder()
                        .setFormat(EmbeddedFormat.BINARY)
                        .setData(IntNode.valueOf(i))
                        .build())
                .build());
      }
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

    // The response should contain 10 entries - some successes and some errors
    // We verify the stream contains the expected number of responses
    String responseBody = response.readEntity(String.class);
    ObjectMapper mapper = new ObjectMapper();
    // Count the number of JSON objects in the response (streaming format)
    int responseCount = 0;
    try (MappingIterator<JsonNode> iterator =
        mapper.readerFor(JsonNode.class).readValues(responseBody)) {
      while (iterator.hasNext()) {
        iterator.next();
        responseCount++;
      }
    }
    assertEquals(10, responseCount);
  }

  @Test
  public void produceInvalidSchemaFormat_recoversWithValidData() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-recovery-schema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send request with invalid schema
    ProduceRequest invalidRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(IntNode.valueOf(42)) // int doesn't match string schema
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
    ErrorResponse errorResponse = invalidResponse.readEntity(ErrorResponse.class);
    assertEquals(400, errorResponse.getErrorCode());

    // Recover with valid data
    ProduceRequest validRequest =
        ProduceRequest.builder()
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.JSONSCHEMA)
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
                testEnv.schemaRegistry().createJsonSchemaDeserializer(),
                testEnv.schemaRegistry().createJsonSchemaDeserializer());
    assertEquals(TextNode.valueOf("valid-data"), produced.value());
  }

  @Test
  public void produceBinaryWithConflictingSchemaOptions_returnsBadRequest() throws Exception {
    String clusterId = testEnv.kafkaCluster().getClusterId();
    String topicName = "error-conflicting-schema-topic";
    testEnv.kafkaCluster().createTopic(topicName, 1, (short) 1);

    // Send request with conflicting schema options (schema_id and schema_version together)
    String request = "{ \"key\": { \"schema_id\": 1, \"schema_version\": 1 } }";

    Response response =
        testEnv.kafkaRest()
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + topicName + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ErrorResponse actual = response.readEntity(ErrorResponse.class);
    assertEquals(400, actual.getErrorCode());

    // Verify we can still produce valid data after the error
    ByteString key = ByteString.copyFromUtf8("after-conflict");
    ByteString value = ByteString.copyFromUtf8("still-working");
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

    ProduceResponse produceResponse = readProduceResponse(validResponse);
    ConsumerRecord<byte[], byte[]> produced =
        testEnv.kafkaCluster()
            .getRecord(
                topicName,
                produceResponse.getPartitionId(),
                produceResponse.getOffset(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    assertEquals(key, ByteString.copyFrom(produced.key()));
    assertEquals(value, ByteString.copyFrom(produced.value()));
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
