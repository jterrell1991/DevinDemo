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

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.GetClusterConfigResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClusterConfigsResourceExpandedIntegrationTest extends ClusterTestHarness {

  public ClusterConfigsResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void updateClusterConfig_thenGetIt_returnsUpdatedValue() {
    String clusterId = getClusterId();

    // First, set a cluster-level config
    Response updateResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"snappy\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    // Now get the cluster config
    testWithRetry(
        () -> {
          Response getResponse =
              request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), getResponse.getStatus());

          GetClusterConfigResponse actual =
              getResponse.readEntity(GetClusterConfigResponse.class);
          assertEquals("snappy", actual.getValue().getValue());
          assertEquals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, actual.getValue().getSource());
        });

    // Reset the config
    Response resetResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());
  }

  @Test
  public void getClusterConfig_notYetSet_returnsNotFound() {
    String clusterId = getClusterId();

    // Cluster-level configs that haven't been explicitly set return not found
    Response response =
        request("/v3/clusters/" + clusterId + "/broker-configs/log.cleaner.threads")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateAndResetClusterConfig_verifyBrokerConfigReflectsChange() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    // Update cluster-level config
    Response updateResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"500\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    // Verify broker-level config reflects the cluster-level change
    testWithRetry(
        () -> {
          Response brokerResponse =
              request(
                  "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                      + "/configs/max.connections")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), brokerResponse.getStatus());

          GetBrokerConfigResponse actual =
              brokerResponse.readEntity(GetBrokerConfigResponse.class);
          assertEquals("500", actual.getValue().getValue());
        });

    // Reset cluster-level config
    Response resetResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs/max.connections")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());
  }

  @Test
  public void updateClusterConfig_nonExistingConfig_throwsNotFound() {
    String clusterId = getClusterId();

    Response response =
        request("/v3/clusters/" + clusterId + "/broker-configs/foobar.nonexistent")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"value\"}", MediaType.APPLICATION_JSON));
    // Non-existing config name should return an error
    assertTrue(
        "Should return error for non-existing config",
        response.getStatus() == Status.NOT_FOUND.getStatusCode()
            || response.getStatus() == Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void resetClusterConfig_notYetSet_throwsNotFound() {
    String clusterId = getClusterId();

    // Resetting a config that was never set at cluster level
    Response response =
        request("/v3/clusters/" + clusterId + "/broker-configs/log.cleaner.threads")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    // Should succeed (no-op) or return not found
    assertTrue(
        "Reset of unset config should be no-content or not-found",
        response.getStatus() == Status.NO_CONTENT.getStatusCode()
            || response.getStatus() == Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void alterConfigBatch_multipleConfigs_updatesAll() {
    String clusterId = getClusterId();

    Response updateResponse =
        request("/v3/clusters/" + clusterId + "/broker-configs:alter")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"data\":["
                        + "{\"name\": \"max.connections\",\"value\":\"2000\"},"
                        + "{\"name\": \"compression.type\",\"value\":\"lz4\"}]}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    // Verify both configs were updated
    testWithRetry(
        () -> {
          Response getResponse1 =
              request("/v3/clusters/" + clusterId + "/broker-configs/max.connections")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), getResponse1.getStatus());
          GetClusterConfigResponse actual1 =
              getResponse1.readEntity(GetClusterConfigResponse.class);
          assertEquals("2000", actual1.getValue().getValue());
        });

    testWithRetry(
        () -> {
          Response getResponse2 =
              request("/v3/clusters/" + clusterId + "/broker-configs/compression.type")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), getResponse2.getStatus());
          GetClusterConfigResponse actual2 =
              getResponse2.readEntity(GetClusterConfigResponse.class);
          assertEquals("lz4", actual2.getValue().getValue());
        });
  }

  @Test
  public void listClusterConfigs_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getClusterConfig_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/broker-configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
