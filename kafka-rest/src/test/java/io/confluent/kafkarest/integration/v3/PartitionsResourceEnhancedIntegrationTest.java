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
import io.confluent.kafkarest.entities.v3.PartitionDataList;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;

public class PartitionsResourceEnhancedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_NAME = "topic-1";
  private static final int NUM_PARTITIONS = 3;

  public PartitionsResourceEnhancedIntegrationTest() {
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
        });
  }

  @Test
  public void listPartitions_multiPartitionTopic_partitionsHaveCorrectIds() {
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
          List<PartitionData> partitions = actual.getValue().getData();
          for (int i = 0; i < NUM_PARTITIONS; i++) {
            final int partitionId = i;
            assertTrue(
                "Partition " + partitionId + " should exist",
                partitions.stream()
                    .anyMatch(p -> p.getPartitionId() == partitionId));
          }
        });
  }

  @Test
  public void getPartition_eachPartition_returnsCorrectLeaderAndReplicas() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    for (int i = 0; i < NUM_PARTITIONS; i++) {
      Response response =
          request(
                  "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/" + i)
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      GetPartitionResponse actual = response.readEntity(GetPartitionResponse.class);
      assertEquals(clusterId, actual.getValue().getClusterId());
      assertEquals(TOPIC_NAME, actual.getValue().getTopicName());
      assertEquals(i, actual.getValue().getPartitionId());

      assertEquals(
          baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME
              + "/partitions/" + i + "/replicas",
          actual.getValue().getReplicas().getRelated());
    }
  }

  @Test
  public void getPartition_multiPartitionTopic_metadataIsCorrect() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetPartitionResponse actual = response.readEntity(GetPartitionResponse.class);
    assertEquals(
        baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/0",
        actual.getValue().getMetadata().getSelf());
    assertEquals(
        "crn:///kafka=" + clusterId + "/topic=" + TOPIC_NAME + "/partition=0",
        actual.getValue().getMetadata().getResourceName().orElse(""));
  }

  @Test
  public void listPartitions_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/non-existing-topic/partitions")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_nonExistingPartitionId_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions/999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getPartition_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/topics/" + TOPIC_NAME + "/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listPartitions_hasCorrectSelfLink() {
    String baseUrl = restConnect;
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
          assertEquals(
              baseUrl + "/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/partitions",
              actual.getValue().getMetadata().getSelf());
        });
  }
}
