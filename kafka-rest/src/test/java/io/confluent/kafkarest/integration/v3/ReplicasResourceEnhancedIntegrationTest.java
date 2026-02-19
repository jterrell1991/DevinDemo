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

import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class ReplicasResourceEnhancedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";

  public ReplicasResourceEnhancedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    HashMap<Integer, List<Integer>> replicas = new HashMap<>();
    replicas.put(0, Arrays.asList(0, 1));
    replicas.put(1, Arrays.asList(1, 2));
    replicas.put(2, Arrays.asList(2, 0));
    createTopic(TOPIC_NAME, replicas);
  }

  @Test
  public void listReplicas_multiReplicaPartition_returnsAllReplicas() {
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          Response response =
              request(
                      "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                          + "/partitions/0/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
          assertEquals(2, actual.getValue().getData().size());
        });
  }

  @Test
  public void listReplicas_exactlyOneLeaderPerPartition() {
    String clusterId = getClusterId();

    for (int partitionId = 0; partitionId < 3; partitionId++) {
      final int pid = partitionId;
      testWithRetry(
          () -> {
            Response response =
                request(
                        "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                            + "/partitions/" + pid + "/replicas")
                    .accept(MediaType.APPLICATION_JSON)
                    .get();
            assertEquals(Status.OK.getStatusCode(), response.getStatus());

            ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
            long leaderCount =
                actual.getValue().getData().stream().filter(ReplicaData::isLeader).count();
            assertEquals(
                "Partition " + pid + " should have exactly one leader", 1, leaderCount);
          });
    }
  }

  @Test
  public void listReplicas_allReplicasInSync() {
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          Response response =
              request(
                      "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                          + "/partitions/0/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
          for (ReplicaData replica : actual.getValue().getData()) {
            assertTrue(
                "Replica on broker " + replica.getBrokerId() + " should be in sync",
                replica.isInSync());
          }
        });
  }

  @Test
  public void listReplicas_differentPartitions_haveDifferentLeaders() {
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          int partition0Leader = -1;
          int partition1Leader = -1;

          Response response0 =
              request(
                      "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                          + "/partitions/0/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response0.getStatus());
          ListReplicasResponse replicas0 = response0.readEntity(ListReplicasResponse.class);
          for (ReplicaData r : replicas0.getValue().getData()) {
            if (r.isLeader()) {
              partition0Leader = r.getBrokerId();
            }
          }

          Response response1 =
              request(
                      "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                          + "/partitions/1/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response1.getStatus());
          ListReplicasResponse replicas1 = response1.readEntity(ListReplicasResponse.class);
          for (ReplicaData r : replicas1.getValue().getData()) {
            if (r.isLeader()) {
              partition1Leader = r.getBrokerId();
            }
          }

          assertTrue("Partition 0 should have a leader", partition0Leader >= 0);
          assertTrue("Partition 1 should have a leader", partition1Leader >= 0);
          assertTrue(
              "Different partitions should have different leaders (spread across brokers)",
              partition0Leader != partition1Leader);
        });
  }

  @Test
  public void getReplica_existingReplica_returnsCorrectBrokerRelationship() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          Response response =
              request(
                      "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                          + "/partitions/0/replicas/0")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          GetReplicaResponse actual = response.readEntity(GetReplicaResponse.class);
          assertEquals(0, actual.getValue().getBrokerId());
          assertEquals(0, actual.getValue().getPartitionId());
          assertEquals(TOPIC_NAME, actual.getValue().getTopicName());
          assertEquals(
              baseUrl + "/v3/clusters/" + clusterId + "/brokers/0",
              actual.getValue().getBroker().getRelated());
        });
  }

  @Test
  public void listReplicas_eachPartition_replicasOnCorrectBrokers() {
    String clusterId = getClusterId();

    int[][] expectedBrokers = {{0, 1}, {1, 2}, {2, 0}};

    for (int partitionId = 0; partitionId < 3; partitionId++) {
      final int pid = partitionId;
      final int[] expected = expectedBrokers[partitionId];

      testWithRetry(
          () -> {
            Response response =
                request(
                        "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                            + "/partitions/" + pid + "/replicas")
                    .accept(MediaType.APPLICATION_JSON)
                    .get();
            assertEquals(Status.OK.getStatusCode(), response.getStatus());

            ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
            List<ReplicaData> replicas = actual.getValue().getData();

            for (int expectedBrokerId : expected) {
              assertTrue(
                  "Partition " + pid + " should have a replica on broker " + expectedBrokerId,
                  replicas.stream()
                      .anyMatch(r -> r.getBrokerId() == expectedBrokerId));
            }
          });
    }
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
  public void listReplicas_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
