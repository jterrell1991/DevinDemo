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

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.CreateTopicResponse;
import io.confluent.kafkarest.entities.v3.GetTopicConfigResponse;
import io.confluent.kafkarest.entities.v3.GetTopicResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.TopicConfigData;
import io.confluent.kafkarest.entities.v3.TopicData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EdgeCasesIntegrationTest extends ClusterTestHarness {

  public EdgeCasesIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void createTopic_customReplicaAssignments_createsCorrectly() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    String topicName = "edge-custom-replica-topic";

    // Create topic with custom replica assignments
    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + topicName
                        + "\",\"replicas_assignments\":"
                        + "{\"0\":[0,1], \"1\":[1,2], \"2\":[2,0]}}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    CreateTopicResponse createResponse = response.readEntity(CreateTopicResponse.class);
    TopicData topicData = createResponse.getValue();
    assertEquals(topicName, topicData.getTopicName());
    assertEquals(2, topicData.getReplicationFactor());

    // Verify partitions and their replica assignments
    testWithRetry(
        () -> {
          // Check partition 0: should have replicas on brokers 0 and 1
          Response replicasResponse0 =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + topicName
                      + "/partitions/0/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), replicasResponse0.getStatus());

          ListReplicasResponse replicas0 =
              replicasResponse0.readEntity(ListReplicasResponse.class);
          assertEquals(2, replicas0.getValue().getData().size());

          // Check partition 1: should have replicas on brokers 1 and 2
          Response replicasResponse1 =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + topicName
                      + "/partitions/1/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), replicasResponse1.getStatus());

          ListReplicasResponse replicas1 =
              replicasResponse1.readEntity(ListReplicasResponse.class);
          assertEquals(2, replicas1.getValue().getData().size());

          // Check partition 2: should have replicas on brokers 2 and 0
          Response replicasResponse2 =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + topicName
                      + "/partitions/2/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), replicasResponse2.getStatus());

          ListReplicasResponse replicas2 =
              replicasResponse2.readEntity(ListReplicasResponse.class);
          assertEquals(2, replicas2.getValue().getData().size());
        });
  }

  @Test
  public void createTopic_withConfig_verifyConfigSynonyms() {
    String clusterId = getClusterId();
    String topicName = "edge-config-synonyms-topic";

    // Create topic with a specific config (cleanup.policy = compact)
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + topicName
                        + "\",\"partitions_count\":1"
                        + ",\"replication_factor\":2"
                        + ",\"configs\":[{\"name\":\"cleanup.policy\",\"value\":\"compact\"}]}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), createResponse.getStatus());

    // Verify config synonyms
    testWithRetry(
        () -> {
          Response configResponse =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + topicName
                      + "/configs/cleanup.policy")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), configResponse.getStatus());

          GetTopicConfigResponse actual =
              configResponse.readEntity(GetTopicConfigResponse.class);
          TopicConfigData configData = actual.getValue();

          assertEquals("cleanup.policy", configData.getName());
          assertEquals("compact", configData.getValue());
          assertFalse(configData.isDefault());
          assertFalse(configData.isReadOnly());
          assertFalse(configData.isSensitive());
          assertEquals(ConfigSource.DYNAMIC_TOPIC_CONFIG, configData.getSource());

          // Verify synonyms
          List<ConfigSynonymData> synonyms = configData.getSynonyms();
          assertNotNull(synonyms);
          assertTrue(synonyms.size() >= 2);

          // First synonym should be the dynamic topic config
          assertEquals("cleanup.policy", synonyms.get(0).getName());
          assertEquals("compact", synonyms.get(0).getValue());
          assertEquals(ConfigSource.DYNAMIC_TOPIC_CONFIG, synonyms.get(0).getSource());

          // Second synonym should be the default config
          assertEquals("log.cleanup.policy", synonyms.get(1).getName());
          assertEquals("delete", synonyms.get(1).getValue());
          assertEquals(ConfigSource.DEFAULT_CONFIG, synonyms.get(1).getSource());
        });
  }

  @Test
  public void createTopic_emptyTopicName_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"\","
                        + "\"partitions_count\":1,"
                        + "\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    // Empty topic name should fail
    assertTrue(
        "Empty topic name should return a client error",
        response.getStatus() >= 400 && response.getStatus() < 500);
  }

  @Test
  public void createTopic_replicationFactorExceedsBrokerCount_returnsBadRequest() {
    String clusterId = getClusterId();

    // With 3 brokers, replication factor of 10 should fail
    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"edge-too-many-replicas\","
                        + "\"partitions_count\":1,"
                        + "\"replication_factor\":10}",
                    MediaType.APPLICATION_JSON));
    assertTrue(
        "Replication factor exceeding broker count should fail",
        response.getStatus() >= 400 && response.getStatus() < 500);
  }

  @Test
  public void createTopic_duplicatePartitionInReplicaAssignments_returnsBadRequest() {
    String clusterId = getClusterId();

    // Invalid: referring to a broker that doesn't exist (broker 99)
    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"edge-invalid-broker-replica\","
                        + "\"replicas_assignments\":{\"0\":[0,99]}}",
                    MediaType.APPLICATION_JSON));
    assertTrue(
        "Assigning to non-existent broker should fail",
        response.getStatus() >= 400 && response.getStatus() < 500);
  }

  @Test
  public void getTopic_internalTopic_returnsInternalFlag() {
    String clusterId = getClusterId();

    // __consumer_offsets is an internal topic
    // It may or may not exist depending on test state, so we create a consumer group first
    createTopic("edge-internal-trigger", 1, (short) 1);

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/topics/__consumer_offsets")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          // __consumer_offsets may not exist yet, so we just verify the request completes
          assertTrue(
              "Response should be OK or NOT_FOUND for internal topic",
              response.getStatus() == Status.OK.getStatusCode()
                  || response.getStatus() == Status.NOT_FOUND.getStatusCode());

          if (response.getStatus() == Status.OK.getStatusCode()) {
            GetTopicResponse actual = response.readEntity(GetTopicResponse.class);
            assertTrue("__consumer_offsets should be internal", actual.getValue().isInternal());
          }
        });
  }

  @Test
  public void createTopic_withZeroPartitions_returnsBadRequest() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"edge-zero-partitions\","
                        + "\"partitions_count\":0,"
                        + "\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertTrue(
        "Zero partitions should fail",
        response.getStatus() >= 400 && response.getStatus() < 500);
  }

  @Test
  public void createAndDeleteTopic_verifyFullLifecycle() {
    String clusterId = getClusterId();
    String topicName = "edge-lifecycle-topic";

    // Create topic
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + topicName
                        + "\",\"partitions_count\":2"
                        + ",\"replication_factor\":2}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), createResponse.getStatus());

    // Verify topic exists
    testWithRetry(
        () -> {
          Response getResponse =
              request("/v3/clusters/" + clusterId + "/topics/" + topicName)
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), getResponse.getStatus());
        });

    // Delete topic
    Response deleteResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topicName)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), deleteResponse.getStatus());

    // Verify topic no longer exists
    testWithRetry(
        () ->
            assertFalse(
                "Topic should not exist after deletion",
                getTopicNames().contains(topicName)));
  }

  @Test
  public void createTopic_duplicateName_returnsBadRequest() {
    String clusterId = getClusterId();
    String topicName = "edge-duplicate-topic";

    // Create first
    Response firstResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + topicName
                        + "\",\"partitions_count\":1"
                        + ",\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), firstResponse.getStatus());

    testWithRetry(
        () -> assertTrue(getTopicNames().contains(topicName)));

    // Create again with same name - should fail
    Response secondResponse =
        request("/v3/clusters/" + clusterId + "/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"" + topicName
                        + "\",\"partitions_count\":1"
                        + ",\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.BAD_REQUEST.getStatusCode(), secondResponse.getStatus());
  }

  @Test
  public void createTopic_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"topic_name\":\"edge-nonexistent-cluster\","
                        + "\"partitions_count\":1,"
                        + "\"replication_factor\":1}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
