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

import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.GetBrokerResponse;
import io.confluent.kafkarest.entities.v3.ListBrokersResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BrokersResourceExpandedIntegrationTest extends ClusterTestHarness {

  public BrokersResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokers_returnsCorrectBrokerCount() {
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
  public void listBrokers_allBrokersHaveUniqueIds() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    Set<Integer> brokerIds = new HashSet<>();
    for (BrokerData broker : actual.getValue().getData()) {
      brokerIds.add(broker.getBrokerId());
    }
    assertEquals(3, brokerIds.size());
  }

  @Test
  public void listBrokers_validateMetadataLinks() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);

    // Verify collection metadata
    assertEquals(
        baseUrl + "/v3/clusters/" + clusterId + "/brokers",
        actual.getValue().getMetadata().getSelf());

    // Verify each broker has correct relationship links
    for (BrokerData broker : actual.getValue().getData()) {
      assertNotNull(broker.getConfigs());
      assertTrue(
          broker.getConfigs().getRelated().contains(
              "/v3/clusters/" + clusterId + "/brokers/" + broker.getBrokerId() + "/configs"));
      assertNotNull(broker.getPartitionReplicas());
      assertTrue(
          broker.getPartitionReplicas().getRelated().contains(
              "/v3/clusters/" + clusterId + "/brokers/" + broker.getBrokerId()
                  + "/partition-replicas"));
    }
  }

  @Test
  public void getBroker_validateBrokerMetadata() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();
    Node firstNode = nodes.get(0);

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + firstNode.id())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerResponse actual = response.readEntity(GetBrokerResponse.class);
    BrokerData broker = actual.getValue();

    assertEquals(clusterId, broker.getClusterId());
    assertEquals(firstNode.id(), broker.getBrokerId());
    assertEquals(firstNode.host(), broker.getHost().orElse(null));
    assertEquals(firstNode.port(), (int) broker.getPort().orElse(0));
  }

  @Test
  public void getBroker_eachBrokerInCluster_returnsBrokerDetails() {
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    for (Node node : nodes) {
      Response response =
          request("/v3/clusters/" + clusterId + "/brokers/" + node.id())
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      GetBrokerResponse actual = response.readEntity(GetBrokerResponse.class);
      assertEquals(node.id(), actual.getValue().getBrokerId());
    }
  }

  @Test
  public void getBroker_nonExistingBrokerId_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_largeBrokerId_returnsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/99999")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void listBrokers_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getBroker_verifyResourceName() {
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();
    int brokerId = nodes.get(0).id();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerResponse actual = response.readEntity(GetBrokerResponse.class);
    assertEquals(
        "crn:///kafka=" + clusterId + "/broker=" + brokerId,
        actual.getValue().getMetadata().getResourceName());
  }
}
