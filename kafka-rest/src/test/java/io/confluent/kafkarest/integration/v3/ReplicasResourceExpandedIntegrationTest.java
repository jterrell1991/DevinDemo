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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.v3.GetReplicaResponse;
import io.confluent.kafkarest.entities.v3.ListReplicasResponse;
import io.confluent.kafkarest.entities.v3.ReplicaData;
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
public class ReplicasResourceExpandedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "replicas-expanded-topic";

  public ReplicasResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    HashMap<Integer, List<Integer>> replicas = new HashMap<>();
    replicas.put(/* partition= */ 0, Arrays.asList(/* leader= */ 0, 1, 2));
    replicas.put(/* partition= */ 1, Arrays.asList(/* leader= */ 1, 2, 0));
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
          // Should have 3 replicas for partition 0
          assertEquals(3, actual.getValue().getData().size());
        });
  }

  @Test
  public void listReplicas_verifyLeaderAndIsrStatus() {
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
          List<ReplicaData> replicas = actual.getValue().getData();

          // Exactly one replica should be the leader
          long leaderCount = replicas.stream().filter(ReplicaData::isLeader).count();
          assertEquals(1, leaderCount);

          // All replicas should be in-sync since all brokers are healthy
          for (ReplicaData replica : replicas) {
            assertTrue(
                "All replicas should be in-sync on a healthy cluster",
                replica.isInSync());
          }
        });
  }

  @Test
  public void listReplicas_differentPartitions_haveDifferentLeaders() {
    String clusterId = getClusterId();

    testWithRetry(
        () -> {
          // Get replicas for partition 0
          Response response0 =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                      + "/partitions/0/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response0.getStatus());
          ListReplicasResponse replicas0 = response0.readEntity(ListReplicasResponse.class);

          // Get replicas for partition 1
          Response response1 =
              request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                      + "/partitions/1/replicas")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response1.getStatus());
          ListReplicasResponse replicas1 = response1.readEntity(ListReplicasResponse.class);

          // Find leaders
          int leader0 = replicas0.getValue().getData().stream()
              .filter(ReplicaData::isLeader)
              .findFirst().get().getBrokerId();
          int leader1 = replicas1.getValue().getData().stream()
              .filter(ReplicaData::isLeader)
              .findFirst().get().getBrokerId();

          // Leaders should be different based on our custom assignment
          // Partition 0 leader should be broker 0, partition 1 leader should be broker 1
          assertEquals(0, leader0);
          assertEquals(1, leader1);
        });
  }

  @Test
  public void getReplica_verifyBrokerRelationshipLink() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/0/replicas/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetReplicaResponse actual = response.readEntity(GetReplicaResponse.class);
    ReplicaData replica = actual.getValue();

    // Verify broker relationship link
    assertNotNull(replica.getBroker());
    assertTrue(
        replica.getBroker().getRelated().contains(
            "/v3/clusters/" + clusterId + "/brokers/0"));
  }

  @Test
  public void getReplica_followerReplica_returnsNonLeader() {
    String clusterId = getClusterId();

    // Broker 1 is a follower for partition 0 (leader is broker 0)
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/0/replicas/1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetReplicaResponse actual = response.readEntity(GetReplicaResponse.class);
    ReplicaData replica = actual.getValue();

    assertEquals(1, replica.getBrokerId());
    assertEquals(false, replica.isLeader());
    assertEquals(true, replica.isInSync());
  }

  @Test
  public void getReplica_nonExistingBrokerReplica_returnsNotFound() {
    String clusterId = getClusterId();

    // Broker 100 doesn't exist
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
                + "/partitions/0/replicas/100")
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
                + "/partitions/100/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listReplicas_nonExistingCluster_returnsNotFound() {
    Response response =
        request(
            "/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0/replicas")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
