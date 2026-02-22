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

import io.confluent.kafkarest.entities.v3.GetReassignmentResponse;
import io.confluent.kafkarest.entities.v3.ListAllReassignmentsResponse;
import io.confluent.kafkarest.entities.v3.ReassignmentData;
import io.confluent.kafkarest.entities.v3.SearchReassignmentsByTopicResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class ReassignmentOperationsExpandedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "reassign-expanded-topic-1";
  private static final String TOPIC_2 = "reassign-expanded-topic-2";

  public ReassignmentOperationsExpandedIntegrationTest() {
    super(/* numBrokers= */ 6, /* withSchemaRegistry= */ false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Map<Integer, List<Integer>> replicaAssignments1 =
        createAssignment(Arrays.asList(0, 1, 2), 50);
    createTopic(TOPIC_1, replicaAssignments1);

    Map<Integer, List<Integer>> replicaAssignments2 =
        createAssignment(Arrays.asList(0, 1, 2), 50);
    createTopic(TOPIC_2, replicaAssignments2);
  }

  @Test
  public void listAllReassignments_multipleTopicsReassigned_returnsAll() {
    String clusterId = getClusterId();

    // Reassign both topics
    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap1 =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_1, 50);
    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap2 =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_2, 50);

    alterPartitionReassignment(reassignmentMap1);
    alterPartitionReassignment(reassignmentMap2);

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/-/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListAllReassignmentsResponse actual =
        response.readEntity(ListAllReassignmentsResponse.class);
    // Should have reassignments from both topics
    assertTrue(
        "Should have reassignments for at least some partitions",
        actual.getValue().getData().size() > 0);
  }

  @Test
  public void searchReassignmentsByTopic_specificTopic_returnsOnlyThatTopic() {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_1, 50);
    alterPartitionReassignment(reassignmentMap);

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1
                + "/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    SearchReassignmentsByTopicResponse actual =
        response.readEntity(SearchReassignmentsByTopicResponse.class);
    for (ReassignmentData data : actual.getValue().getData()) {
      assertEquals(TOPIC_1, data.getTopicName());
    }
  }

  @Test
  public void getReassignment_specificPartition_returnsReassignment() {
    String clusterId = getClusterId();

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap =
        createReassignment(Arrays.asList(3, 4, 5), TOPIC_1, 50);
    alterPartitionReassignment(reassignmentMap);

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1
                + "/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetReassignmentResponse actual = response.readEntity(GetReassignmentResponse.class);
    assertEquals(TOPIC_1, actual.getValue().getTopicName());
    assertEquals(0, actual.getValue().getPartitionId());
    assertEquals(
        Arrays.asList(3, 4, 5),
        actual.getValue().getAddingReplicas());
  }

  @Test
  public void searchReassignmentsByTopic_nonExistingTopic_returnsEmpty() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/nonexistent-topic"
                + "/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    SearchReassignmentsByTopicResponse actual =
        response.readEntity(SearchReassignmentsByTopicResponse.class);
    assertTrue(actual.getValue().getData().isEmpty());
  }

  @Test
  public void getReassignment_nonReassignedPartition_returnsNotFound() {
    String clusterId = getClusterId();

    // Don't trigger any reassignment - just query for one
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_2
                + "/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    // If no reassignment is in progress, should return not found
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignment_nonExistingCluster_returnsNotFound() {
    Response response =
        request(
            "/v3/clusters/foobar/topics/" + TOPIC_1
                + "/partitions/0/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getReassignment_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/topics/" + TOPIC_1
                + "/partitions/9999/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listAllReassignments_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/-/partitions/-/reassignment")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
