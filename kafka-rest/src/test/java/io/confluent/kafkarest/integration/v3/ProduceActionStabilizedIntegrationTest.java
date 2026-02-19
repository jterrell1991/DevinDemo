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

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProduceActionStabilizedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "produce-test-topic";

  public ProduceActionStabilizedIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ true);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @Test
  public void produceBinary_validPayload_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceBinaryWithNullData_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":null},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":null}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceBinaryWithInvalidData_returnsBadRequest() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":1},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"fooba\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          String body = response.readEntity(String.class);
          assertTrue("Should contain error code 400", body.contains("400"));
        });
  }

  @Test
  public void produceJson_validPayload_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"JSON\",\"data\":\"foo\"},"
            + "\"value\":{\"type\":\"JSON\",\"data\":\"bar\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceJsonWithNullData_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"JSON\",\"data\":null},"
            + "\"value\":{\"type\":\"JSON\",\"data\":null}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceJsonWithObjectData_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"JSON\",\"data\":{\"id\":1}},"
            + "\"value\":{\"type\":\"JSON\",\"data\":{\"name\":\"test\"}}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceAvroWithRawSchema_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"data\":\"foo\"},"
            + "\"value\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"data\":\"bar\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceAvroWithInvalidData_returnsBadRequest() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"data\":1},"
            + "\"value\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"data\":2}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          String body = response.readEntity(String.class);
          assertTrue("Should contain error code 400", body.contains("400"));
        });
  }

  @Test
  public void produceWithInvalidDataFormat_returnsBadRequest() {
    String clusterId = getClusterId();
    String request = "{ \"records\": {\"subject\": \"foobar\" } }";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          String body = response.readEntity(String.class);
          assertTrue("Should contain error code 400", body.contains("400"));
        });
  }

  @Test
  public void produce_malformedJson_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{invalid json", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void produce_emptyBody_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{}", MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void produce_nonExistingTopic_returnsError() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"}}";

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/nonexistent-topic/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String body = response.readEntity(String.class);
    assertTrue(
        "Should contain error for non-existing topic",
        body.contains("error_code"));
  }

  @Test
  public void produce_nonExistingCluster_returnsNotFound() {
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"}}";

    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void produce_withPartitionId_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"partition_id\":0,"
            + "\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produce_withHeaders_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"},"
            + "\"value\":{\"type\":\"BINARY\",\"data\":\"YmFy\"},"
            + "\"headers\":[{\"name\":\"header-1\",\"value\":\"dmFsdWUtMQ==\"}]}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceAvroWithSchemaAndSubjectStrategy_returns200() {
    String clusterId = getClusterId();
    String request =
        "{\"key\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"subject_name_strategy\":\"TOPIC_NAME\","
            + "\"data\":\"foo\"},"
            + "\"value\":{\"type\":\"AVRO\","
            + "\"schema\":\"{ \\\"type\\\": \\\"string\\\" }\","
            + "\"subject_name_strategy\":\"TOPIC_NAME\","
            + "\"data\":\"bar\"}}";

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
                  .accept(MediaType.APPLICATION_JSON)
                  .post(Entity.entity(request, MediaType.APPLICATION_JSON));
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void produceAvroWithRawSchemaAndSchemaVersion_returnsBadRequest() {
    String clusterId = getClusterId();
    String request =
        "{ \"key\": { \"schema\": \"{ \\\"type\\\": \\\"string\\\" }\","
            + " \"schema_version\": 1 } }";

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    String body = response.readEntity(String.class);
    assertTrue("Should contain error code 400", body.contains("400"));
  }
}
