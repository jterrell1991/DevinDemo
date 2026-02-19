/*
 * Copyright 2020 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class ErrorScenariosIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public ErrorScenariosIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_NAME, 1, (short) 1);
  }

  @Test
  public void createTopic_malformedJson_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{invalid json", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_emptyBody_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{}", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_missingTopicName_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"partitions_count\":1,\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/topics/" + TOPIC_NAME)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopic_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/non-existing-topic")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void deleteTopic_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/non-existing-topic")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReplica_nonExistingReplica_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/partitions/0/replicas/999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroup_nonExistingGroup_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/non-existing-group")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getTopicConfig_nonExistingConfig_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/configs/non.existing.config")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateTopicConfig_malformedJson_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/configs/cleanup.policy")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{not valid json}", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_existingTopicName_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + TOPIC_NAME
                        + "\",\"partitions_count\":1,\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void listTopics_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/topics")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listBrokers_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listPartitions_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/foobar/partitions")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/partitions/999/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listConsumerGroups_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/consumer-groups")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_invalidReplicationFactor_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"new-topic\",\"partitions_count\":1,"
                        + "\"replication_factor\":99}",
                    MediaType.APPLICATION_JSON));
    assertTrue(
        "Should return error for invalid replication factor",
        response.getStatus() >= 400);
  }

  @Test
  public void alterTopicConfigBatch_malformedJson_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/configs:alter")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{malformed}", MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignment_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignment_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/partitions/999/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void searchReplicasByBroker_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/999/partition-replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBrokerConfig_nonExistingBroker_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/999/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/consumer-groups/foobar/lags/"
                    + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"new-topic\",\"partitions_count\":1,"
                        + "\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void deleteTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/non-existing-cluster/topics/" + TOPIC_NAME)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void createTopic_unrecognizedField_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"new-topic\",\"unknown_field\":\"value\","
                        + "\"partitions_count\":1,\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertTrue(
        "Should return error for unrecognized fields",
        response.getStatus() >= 400);
  }
}
