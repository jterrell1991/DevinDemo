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
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.v3.GetPartitionResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.PartitionData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionsResourceExpandedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "partitions-expanded-topic";
  private static final int NUM_PARTITIONS = 3;

  public PartitionsResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_NAME, NUM_PARTITIONS, (short) 2);
  }

  @Test
  public void listPartitions_multiPartitionTopic_returnsAllPartitions() {
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListPartitionsResponse actual = response.readEntity(ListPartitionsResponse.class);
          assertEquals(NUM_PARTITIONS, actual.getValue().getData().size());

          // Verify each partition has correct metadata
          for (int i = 0; i < NUM_PARTITIONS; i++) {
            PartitionData partition = actual.getValue().getData().get(i);
            assertEquals(clusterId, partition.getClusterId());
            assertEquals(TOPIC_NAME, partition.getTopicName());
            assertEquals(i, partition.getPartitionId());
          }
        });
  }

  @Test
  public void getPartition_eachPartitionInMultiPartitionTopic_returnsCorrectPartition() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
      final int pid = partitionId;
      Response response =
          request(
              "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                  + "/partitions/" + partitionId)
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      GetPartitionResponse actual = response.readEntity(GetPartitionResponse.class);
      assertEquals(clusterId, actual.getValue().getClusterId());
      assertEquals(TOPIC_NAME, actual.getValue().getTopicName());
      assertEquals(pid, actual.getValue().getPartitionId());
    }
  }

  @Test
  public void getPartition_outOfRangePartitionId_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/" + NUM_PARTITIONS)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listPartitions_topicWithCustomReplicaAssignments_returnsPartitions() {
    String clusterId = getClusterId();
    String customTopic = "custom-replica-partitions-topic";

    HashMap<Integer, List<Integer>> replicas = new HashMap<>();
    replicas.put(0, Arrays.asList(0, 1));
    replicas.put(1, Arrays.asList(1, 2));
    replicas.put(2, Arrays.asList(2, 0));
    createTopic(customTopic, replicas);

    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + customTopic + "/partitions")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListPartitionsResponse actual = response.readEntity(ListPartitionsResponse.class);
          assertEquals(3, actual.getValue().getData().size());
        });
  }

  @Test
  public void getPartition_verifyRelationshipLinks() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetPartitionResponse actual = response.readEntity(GetPartitionResponse.class);
    PartitionData partition = actual.getValue();

    // Verify relationship links are present and correctly formed
    assertTrue(
        partition.getReplicas().getRelated().contains(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/0/replicas"));
    assertTrue(
        partition.getReassignment().getRelated().contains(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/0/reassignment"));
  }

  @Test
  public void listPartitions_afterTopicDeletion_returnsNotFound() {
    String clusterId = getClusterId();
    String tempTopic = "temp-partitions-topic";
    createTopic(tempTopic, 2, (short) 1);

    // Verify partitions exist
    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + tempTopic + "/partitions")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());
        });

    // Delete the topic
    Response deleteResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + tempTopic)
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), deleteResponse.getStatus());

    // Verify partitions are no longer accessible
    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + tempTopic + "/partitions")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        });
  }

  @Test
  public void getPartition_negativePartitionId_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/-1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    // Negative partition IDs should return not found
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
