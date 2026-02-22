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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.v3.ConsumerAssignmentData;
import io.confluent.kafkarest.entities.v3.GetConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerAssignmentsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerAssignmentsResourceExpandedIntegrationTest extends ClusterTestHarness {

  public ConsumerAssignmentsResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listConsumerAssignments_multipleTopics_returnsAllAssignments() {
    String clusterId = getClusterId();

    createTopic("assign-topic-1", /* numPartitions= */ 2, /* replicationFactor= */ (short) 1);
    createTopic("assign-topic-2", /* numPartitions= */ 2, /* replicationFactor= */ (short) 1);

    KafkaConsumer<?, ?> consumer = createConsumer("assign-group-1", "assign-client-1");
    consumer.subscribe(Arrays.asList("assign-topic-1", "assign-topic-2"));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/assign-group-1/consumers/"
                + consumer.groupMetadata().memberId() + "/assignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListConsumerAssignmentsResponse actual =
        response.readEntity(ListConsumerAssignmentsResponse.class);

    // Consumer should be assigned partitions from both topics
    assertEquals(4, actual.getValue().getData().size());

    // Verify assignment covers both topics
    Set<String> assignedTopics = new HashSet<>();
    for (ConsumerAssignmentData assignment : actual.getValue().getData()) {
      assignedTopics.add(assignment.getTopicName());
    }
    assertTrue(assignedTopics.contains("assign-topic-1"));
    assertTrue(assignedTopics.contains("assign-topic-2"));

    consumer.close();
  }

  @Test
  public void getConsumerAssignment_verifyRelationshipLinks() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("assign-link-topic", /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("assign-link-group", "assign-link-client");
    consumer.subscribe(Collections.singletonList("assign-link-topic"));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/assign-link-group"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/assign-link-topic/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetConsumerAssignmentResponse actual =
        response.readEntity(GetConsumerAssignmentResponse.class);
    ConsumerAssignmentData assignment = actual.getValue();

    // Verify partition relationship link
    assertNotNull(assignment.getPartition());
    assertTrue(
        assignment.getPartition().getRelated().contains("/topics/assign-link-topic/partitions/0"));

    // Verify lag relationship link
    assertNotNull(assignment.getLag());
    assertTrue(
        assignment.getLag().getRelated().contains("/lags/assign-link-topic/partitions/0"));

    consumer.close();
  }

  @Test
  public void listConsumerAssignments_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
            "/v3/clusters/" + clusterId
                + "/consumer-groups/nonexistent-group/consumers/foobar/assignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerAssignment_nonExistingPartition_returnsNotFound() {
    String clusterId = getClusterId();

    createTopic("assign-nopart-topic", /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("assign-nopart-group", "assign-nopart-client");
    consumer.subscribe(Collections.singletonList("assign-nopart-topic"));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));

    // Partition 99 doesn't exist
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/assign-nopart-group"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/assign-nopart-topic/partitions/99")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    consumer.close();
  }

  @Test
  public void listConsumerAssignments_nonExistingCluster_returnsNotFound() {
    createTopic("assign-nocluster-topic", /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("assign-nocluster-group", "assign-nocluster-client");
    consumer.subscribe(Collections.singletonList("assign-nocluster-topic"));
    consumer.poll(Duration.ofSeconds(5));

    Response response =
        request(
            "/v3/clusters/foobar/consumer-groups/assign-nocluster-group/consumers/"
                + consumer.groupMetadata().memberId() + "/assignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    consumer.close();
  }

  @Test
  public void getConsumerAssignment_nonExistingTopic_returnsNotFound() {
    String clusterId = getClusterId();

    createTopic("assign-real-topic", /* numPartitions= */ 1, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("assign-notopic-group", "assign-notopic-client");
    consumer.subscribe(Collections.singletonList("assign-real-topic"));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));

    // Non-existing topic assignment
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/assign-notopic-group"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/nonexistent-topic/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    consumer.close();
  }

  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    properties.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }
}
