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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerLagsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerLagsResourceExpandedIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "lag-expanded-topic-1";
  private static final String TOPIC_2 = "lag-expanded-topic-2";
  private static final String TOPIC_3 = "lag-expanded-topic-3";
  private static final String GROUP_1 = "lag-expanded-group-1";
  private static final int NUM_PARTITIONS = 3;
  private String clusterId;

  private final List<BinaryPartitionProduceRecord> produceRecords =
      Arrays.asList(
          new BinaryPartitionProduceRecord(null, "value-1"),
          new BinaryPartitionProduceRecord(null, "value-2"),
          new BinaryPartitionProduceRecord(null, "value-3"));

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clusterId = getClusterId();
    createTopic(TOPIC_1, NUM_PARTITIONS, (short) 1);
    createTopic(TOPIC_2, NUM_PARTITIONS, (short) 1);
    createTopic(TOPIC_3, NUM_PARTITIONS, (short) 1);
  }

  @Test
  public void listConsumerLags_multiplePartitionsAndTopics_returnsAllLags() {
    // Produce to multiple partitions across topics
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(produceRecords);
    produce(TOPIC_1, 0, request);
    produce(TOPIC_1, 1, request);
    produce(TOPIC_2, 0, request);

    KafkaConsumer<?, ?> consumer = createConsumer(GROUP_1, "lag-client-1");
    consumer.subscribe(Arrays.asList(TOPIC_1, TOPIC_2));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/consumer-groups/" + GROUP_1 + "/lags")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListConsumerLagsResponse actual = response.readEntity(ListConsumerLagsResponse.class);
          ConsumerLagDataList lagList = actual.getValue();

          // Should have lags for all subscribed partitions (TOPIC_1: 3 + TOPIC_2: 3 = 6)
          assertEquals(NUM_PARTITIONS * 2, lagList.getData().size());
        });

    consumer.close();
  }

  @Test
  public void getConsumerLag_afterFullConsumption_returnsZeroLag() {
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(produceRecords);
    produce(TOPIC_1, 0, request);

    KafkaConsumer<?, ?> consumer = createConsumer("lag-zero-group", "lag-zero-client");
    consumer.subscribe(Collections.singletonList(TOPIC_1));
    // Consume all messages
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/consumer-groups/lag-zero-group"
                      + "/lags/" + TOPIC_1 + "/partitions/0")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ConsumerLagData lagData =
              response.readEntity(GetConsumerLagResponse.class).getValue();
          assertEquals(0, (long) lagData.getLag());
          assertEquals(3L, (long) lagData.getCurrentOffset());
          assertEquals(3L, (long) lagData.getLogEndOffset());
        });

    consumer.close();
  }

  @Test
  public void getConsumerLag_withPositiveLag_returnsCorrectValues() {
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(produceRecords);
    produce(TOPIC_2, 0, request);

    KafkaConsumer<?, ?> consumer = createConsumer("lag-positive-group", "lag-positive-client");
    consumer.subscribe(Collections.singletonList(TOPIC_2));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    // Produce more without consuming
    produce(TOPIC_2, 0, request);

    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId + "/consumer-groups/lag-positive-group"
                      + "/lags/" + TOPIC_2 + "/partitions/0")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ConsumerLagData lagData =
              response.readEntity(GetConsumerLagResponse.class).getValue();
          assertEquals(3L, (long) lagData.getLag());
          assertEquals(3L, (long) lagData.getCurrentOffset());
          assertEquals(6L, (long) lagData.getLogEndOffset());
        });

    consumer.close();
  }

  @Test
  public void listConsumerLags_multipleConsumersInGroup_returnsLagsForAll() {
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(produceRecords);
    produce(TOPIC_3, 0, request);
    produce(TOPIC_3, 1, request);

    KafkaConsumer<?, ?> consumer1 = createConsumer("lag-multi-group", "lag-multi-client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("lag-multi-group", "lag-multi-client-2");

    consumer1.subscribe(Collections.singletonList(TOPIC_3));
    consumer2.subscribe(Collections.singletonList(TOPIC_3));
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));
    consumer1.commitSync();
    consumer2.commitSync();

    testWithRetry(
        () -> {
          Response response =
              request(
                  "/v3/clusters/" + clusterId
                      + "/consumer-groups/lag-multi-group/lags")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response.getStatus());

          ListConsumerLagsResponse actual =
              response.readEntity(ListConsumerLagsResponse.class);
          // Should have lag entries for all partitions
          assertEquals(NUM_PARTITIONS, actual.getValue().getData().size());
        });

    consumer1.close();
    consumer2.close();
  }

  @Test
  public void getConsumerLag_nonExistingTopic_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer("lag-notopic-group", "lag-notopic-client");
    consumer.subscribe(Collections.singletonList(TOPIC_1));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/lag-notopic-group"
                + "/lags/nonexistent-topic/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    consumer.close();
  }

  @Test
  public void getConsumerLag_nonExistingPartition_returnsNotFound() {
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(produceRecords);
    produce(TOPIC_1, 0, request);

    KafkaConsumer<?, ?> consumer = createConsumer("lag-nopart-group", "lag-nopart-client");
    consumer.subscribe(Collections.singletonList(TOPIC_1));
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/lag-nopart-group"
                + "/lags/" + TOPIC_1 + "/partitions/999")
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
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }
}
