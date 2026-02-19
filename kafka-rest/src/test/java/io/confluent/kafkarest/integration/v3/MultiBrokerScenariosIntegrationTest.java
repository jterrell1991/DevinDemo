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

import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.ListPartitionsResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
import io.confluent.kafkarest.entities.v3.SearchReplicasByBrokerResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MultiBrokerScenariosIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "multi-broker-topic";
  private static final int NUM_PARTITIONS = 6;

  public MultiBrokerScenariosIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    HashMap<Integer, List<Integer>> replicas = new HashMap<>();
    replicas.put(0, Arrays.asList(0, 1, 2));
    replicas.put(1, Arrays.asList(1, 2, 0));
    replicas.put(2, Arrays.asList(2, 0, 1));
    replicas.put(3, Arrays.asList(0, 2, 1));
    replicas.put(4, Arrays.asList(1, 0, 2));
    replicas.put(5, Arrays.asList(2, 1, 0));
    createTopic(TOPIC_NAME, replicas);
  }

  @Test
  public void listBrokers_allBrokersAvailable() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    assertEquals(3, actual.getValue().getData().size());
  }

  @Test
  public void listPartitions_allPartitionsPresent() {
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
        });
  }

  @Test
  public void replicas_allReplicasInSync_acrossAllPartitions() {
    String clusterId = getClusterId();

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
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
            assertEquals(
                "Partition " + pid + " should have 3 replicas",
                3,
                actual.getValue().getData().size());

            for (ReplicaData replica : actual.getValue().getData()) {
              assertTrue(
                  "Replica on broker " + replica.getBrokerId() + " for partition " + pid
                      + " should be in sync",
                  replica.isInSync());
            }
          });
    }
  }

  @Test
  public void replicas_leadersDistributedAcrossBrokers() {
    String clusterId = getClusterId();

    Set<Integer> leaderBrokers = new HashSet<>();

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
      Response response =
          request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                      + "/partitions/" + partitionId + "/replicas")
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
      for (ReplicaData replica : actual.getValue().getData()) {
        if (replica.isLeader()) {
          leaderBrokers.add(replica.getBrokerId());
        }
      }
    }

    assertTrue(
        "Leaders should be distributed across multiple brokers, found leaders on "
            + leaderBrokers.size() + " broker(s)",
        leaderBrokers.size() > 1);
  }

  @Test
  public void searchReplicasByBroker_eachBrokerHasReplicas() {
    String clusterId = getClusterId();

    for (int brokerId = 0; brokerId < 3; brokerId++) {
      Response response =
          request(
                  "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/partition-replicas")
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      SearchReplicasByBrokerResponse actual =
          response.readEntity(SearchReplicasByBrokerResponse.class);
      assertTrue(
          "Broker " + brokerId + " should have replicas assigned",
          actual.getValue().getData().size() > 0);
    }
  }

  @Test
  public void searchReplicasByBroker_allBrokersHaveEqualReplicaCount() {
    String clusterId = getClusterId();

    int[] replicaCounts = new int[3];

    for (int brokerId = 0; brokerId < 3; brokerId++) {
      Response response =
          request(
                  "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/partition-replicas")
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      SearchReplicasByBrokerResponse actual =
          response.readEntity(SearchReplicasByBrokerResponse.class);
      replicaCounts[brokerId] = actual.getValue().getData().size();
    }

    assertEquals(
        "All brokers should have the same number of replicas",
        replicaCounts[0],
        replicaCounts[1]);
    assertEquals(
        "All brokers should have the same number of replicas",
        replicaCounts[1],
        replicaCounts[2]);
  }

  @Test
  public void partitionReassignment_getReassignment_nonReassigningPartition() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                    + "/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    GetReassignmentResponse actual = response.readEntity(GetReassignmentResponse.class);
    assertTrue(
        "Non-reassigning partition should have empty adding replicas",
        actual.getValue().getAddingReplicas().isEmpty());
    assertTrue(
        "Non-reassigning partition should have empty removing replicas",
        actual.getValue().getRemovingReplicas().isEmpty());
  }

  @Test
  public void partitionReassignment_getReassignment_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/foobar/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void partitionReassignment_getReassignment_nonExistingPartition_returnsNotFound() {
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
  public void replicas_correctLeadersMatchAssignment() {
    String clusterId = getClusterId();

    int[] expectedLeaders = {0, 1, 2, 0, 1, 2};

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
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
            int leaderBrokerId = -1;
            for (ReplicaData replica : actual.getValue().getData()) {
              if (replica.isLeader()) {
                leaderBrokerId = replica.getBrokerId();
              }
            }
            assertEquals(
                "Partition " + pid + " should have leader on broker " + expectedLeaders[pid],
                expectedLeaders[pid],
                leaderBrokerId);
          });
    }
  }

  @Test
  public void replicas_allBrokersHostReplicasForTopic() {
    String clusterId = getClusterId();

    Set<Integer> brokerIds = new HashSet<>();

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
      Response response =
          request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                      + "/partitions/" + partitionId + "/replicas")
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      ListReplicasResponse actual = response.readEntity(ListReplicasResponse.class);
      for (ReplicaData replica : actual.getValue().getData()) {
        brokerIds.add(replica.getBrokerId());
      }
    }

    assertEquals("All 3 brokers should host replicas", 3, brokerIds.size());
    assertTrue("Broker 0 should have replicas", brokerIds.contains(0));
    assertTrue("Broker 1 should have replicas", brokerIds.contains(1));
    assertTrue("Broker 2 should have replicas", brokerIds.contains(2));
  }
}
