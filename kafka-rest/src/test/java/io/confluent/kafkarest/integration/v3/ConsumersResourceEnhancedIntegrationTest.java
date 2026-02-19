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

import io.confluent.kafkarest.entities.v3.ConsumerData;
import io.confluent.kafkarest.entities.v3.GetConsumerResponse;
import io.confluent.kafkarest.entities.v3.ListConsumersResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
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
public class ConsumersResourceEnhancedIntegrationTest extends ClusterTestHarness {

  public ConsumersResourceEnhancedIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listConsumers_multipleConsumers_returnsAllConsumers() {
    String clusterId = getClusterId();

    createTopic("topic-1", 3, (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    consumer1.subscribe(Arrays.asList("topic-1"));
    consumer2.subscribe(Arrays.asList("topic-1"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListConsumersResponse actual = response.readEntity(ListConsumersResponse.class);
    assertEquals(2, actual.getValue().getData().size());
  }

  @Test
  public void listConsumers_eachConsumerHasCorrectClientId() {
    String clusterId = getClusterId();

    createTopic("topic-1", 3, (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    consumer1.subscribe(Arrays.asList("topic-1"));
    consumer2.subscribe(Arrays.asList("topic-1"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListConsumersResponse actual = response.readEntity(ListConsumersResponse.class);
    List<String> clientIds =
        actual.getValue().getData().stream()
            .map(ConsumerData::getClientId)
            .collect(Collectors.toList());
    assertTrue("Should contain client-1", clientIds.contains("client-1"));
    assertTrue("Should contain client-2", clientIds.contains("client-2"));
  }

  @Test
  public void getConsumer_returnsConsumerWithAssignmentsLink() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", 3, (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    consumer1.subscribe(Arrays.asList("topic-1"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer1.poll(Duration.ofSeconds(1));

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1"
                    + "/consumers/" + consumer1.groupMetadata().memberId())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetConsumerResponse actual = response.readEntity(GetConsumerResponse.class);
    assertEquals("consumer-group-1", actual.getValue().getConsumerGroupId());
    assertEquals("client-1", actual.getValue().getClientId());
    assertNotNull(actual.getValue().getAssignments());
    assertEquals(
        baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers/"
            + consumer1.groupMetadata().memberId() + "/assignments",
        actual.getValue().getAssignments().getRelated());
  }

  @Test
  public void getConsumer_differentConsumersInSameGroup_returnDifferentIds() {
    String clusterId = getClusterId();

    createTopic("topic-1", 3, (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    consumer1.subscribe(Arrays.asList("topic-1"));
    consumer2.subscribe(Arrays.asList("topic-1"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));

    String memberId1 = consumer1.groupMetadata().memberId();
    String memberId2 = consumer2.groupMetadata().memberId();
    assertTrue("Consumer member IDs should be different", !memberId1.equals(memberId2));

    Response response1 =
        request(
                "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers/"
                    + memberId1)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response1.getStatus());
    GetConsumerResponse actual1 = response1.readEntity(GetConsumerResponse.class);
    assertEquals("client-1", actual1.getValue().getClientId());

    Response response2 =
        request(
                "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers/"
                    + memberId2)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
    GetConsumerResponse actual2 = response2.readEntity(GetConsumerResponse.class);
    assertEquals("client-2", actual2.getValue().getClientId());
  }

  @Test
  public void listConsumers_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/foobar/consumers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listConsumers_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/consumer-groups/consumer-group-1/consumers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumer_nonExistingConsumer_returnsNotFound() {
    String clusterId = getClusterId();

    createTopic("topic-1", 3, (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    consumer1.subscribe(Arrays.asList("topic-1"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer1.poll(Duration.ofSeconds(1));

    Response response =
        request(
                "/v3/clusters/" + clusterId
                    + "/consumer-groups/consumer-group-1/consumers/nonexistent-member-id")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumer_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request(
                "/v3/clusters/" + clusterId + "/consumer-groups/foobar/consumers/some-member")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
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
