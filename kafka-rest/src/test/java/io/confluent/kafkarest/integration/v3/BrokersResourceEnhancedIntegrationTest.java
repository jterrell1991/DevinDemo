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
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.Node;
import org.junit.Test;

public class BrokersResourceEnhancedIntegrationTest extends ClusterTestHarness {

  public BrokersResourceEnhancedIntegrationTest() {
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
  public void listBrokers_eachBrokerHasHostAndPort() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    for (BrokerData broker : actual.getValue().getData()) {
      assertTrue("Broker host should be present", broker.getHost().isPresent());
      assertTrue("Broker port should be present and positive",
          broker.getPort().isPresent() && broker.getPort().get() > 0);
    }
  }

  @Test
  public void listBrokers_eachBrokerHasConfigsRelationship() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    for (BrokerData broker : actual.getValue().getData()) {
      assertEquals(
          baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + broker.getBrokerId() + "/configs",
          broker.getConfigs().getRelated());
    }
  }

  @Test
  public void listBrokers_eachBrokerHasPartitionReplicasRelationship() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    for (BrokerData broker : actual.getValue().getData()) {
      assertEquals(
          baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + broker.getBrokerId()
              + "/partition-replicas",
          broker.getPartitionReplicas().getRelated());
    }
  }

  @Test
  public void getBroker_eachBroker_returnsCorrectMetadata() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();
    ArrayList<Node> nodes = getBrokers();

    for (Node node : nodes) {
      Response response =
          request("/v3/clusters/" + clusterId + "/brokers/" + node.id())
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(Status.OK.getStatusCode(), response.getStatus());

      GetBrokerResponse actual = response.readEntity(GetBrokerResponse.class);
      assertEquals(clusterId, actual.getValue().getClusterId());
      assertEquals(node.id(), actual.getValue().getBrokerId());
      assertEquals(node.host(), actual.getValue().getHost().orElse(null));
      assertEquals(Integer.valueOf(node.port()), actual.getValue().getPort().orElse(null));
      assertEquals(
          baseUrl + "/v3/clusters/" + clusterId + "/brokers/" + node.id(),
          actual.getValue().getMetadata().getSelf());
    }
  }

  @Test
  public void listBrokers_brokerIdsAreUnique() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    List<BrokerData> brokers = actual.getValue().getData();
    long uniqueIds = brokers.stream().map(BrokerData::getBrokerId).distinct().count();
    assertEquals(brokers.size(), uniqueIds);
  }

  @Test
  public void listBrokers_hasCorrectSelfLink() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokersResponse actual = response.readEntity(ListBrokersResponse.class);
    assertEquals(
        baseUrl + "/v3/clusters/" + clusterId + "/brokers",
        actual.getValue().getMetadata().getSelf());
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
  public void getBroker_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/brokers/0")
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
}
