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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafkarest.entities.ConfigSource;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.ConfigSynonymData;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BrokerConfigsResourceExpandedIntegrationTest extends ClusterTestHarness {

  public BrokerConfigsResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listBrokerConfigs_returnsNonEmptyConfigList() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListBrokerConfigsResponse actual = response.readEntity(ListBrokerConfigsResponse.class);
    assertTrue(
        "Broker should have at least one config",
        actual.getValue().getData().size() > 0);
  }

  @Test
  public void getBrokerConfig_readOnlyConfig_hasReadOnlyFlag() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    // broker.id is a read-only config
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/broker.id")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerConfigResponse actual = response.readEntity(GetBrokerConfigResponse.class);
    assertTrue("broker.id should be read-only", actual.getValue().isReadOnly());
  }

  @Test
  public void getBrokerConfig_sensitiveConfig_hasSensitiveFlag() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    // Check a config that's expected to exist - ssl.keystore.password is sensitive
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/ssl.keystore.password")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerConfigResponse actual = response.readEntity(GetBrokerConfigResponse.class);
    assertTrue("ssl.keystore.password should be sensitive", actual.getValue().isSensitive());
  }

  @Test
  public void getBrokerConfig_defaultConfig_hasDefaultFlag() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/log.cleaner.threads")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerConfigResponse actual = response.readEntity(GetBrokerConfigResponse.class);
    assertTrue(
        "log.cleaner.threads should be default when not overridden",
        actual.getValue().isDefault());
    assertEquals(ConfigSource.DEFAULT_CONFIG, actual.getValue().getSource());
  }

  @Test
  public void getBrokerConfig_withSynonyms_returnsSynonyms() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetBrokerConfigResponse actual = response.readEntity(GetBrokerConfigResponse.class);
    assertNotNull(actual.getValue().getSynonyms());
    assertFalse(
        "Config should have at least one synonym",
        actual.getValue().getSynonyms().isEmpty());

    // The first synonym should match the config itself for default configs
    ConfigSynonymData synonym = actual.getValue().getSynonyms().get(0);
    assertEquals("compression.type", synonym.getName());
  }

  @Test
  public void updateBrokerConfig_thenVerifySynonymsIncludeBothSources() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    // Update a config
    Response updateResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"gzip\"}", MediaType.APPLICATION_JSON));
    assertEquals(Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

    // Verify synonyms include both dynamic and default sources
    testWithRetry(
        () -> {
          Response getResponse =
              request(
                  "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                      + "/configs/compression.type")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), getResponse.getStatus());

          GetBrokerConfigResponse actual =
              getResponse.readEntity(GetBrokerConfigResponse.class);
          assertEquals("gzip", actual.getValue().getValue());
          assertFalse(actual.getValue().isDefault());
          assertEquals(ConfigSource.DYNAMIC_BROKER_CONFIG, actual.getValue().getSource());

          // Should have both dynamic broker config and default config in synonyms
          List<ConfigSynonymData> synonyms = actual.getValue().getSynonyms();
          assertTrue(synonyms.size() >= 2);
          assertEquals(ConfigSource.DYNAMIC_BROKER_CONFIG, synonyms.get(0).getSource());
          assertEquals(ConfigSource.DEFAULT_CONFIG, synonyms.get(1).getSource());
        });

    // Reset the config
    Response resetResponse =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/compression.type")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.NO_CONTENT.getStatusCode(), resetResponse.getStatus());
  }

  @Test
  public void alterConfigBatch_withInvalidConfigName_throwsBadRequest() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs:alter")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"data\":["
                        + "{\"name\": \"nonexistent.config.name\",\"value\":\"value\"}]}",
                    MediaType.APPLICATION_JSON));
    // Invalid config should fail
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void updateBrokerConfig_readOnlyConfig_throwsBadRequest() {
    String clusterId = getClusterId();
    int brokerId = getBrokers().get(0).id();

    // broker.id is read-only and cannot be updated
    Response response =
        request(
            "/v3/clusters/" + clusterId + "/brokers/" + brokerId
                + "/configs/broker.id")
            .accept(MediaType.APPLICATION_JSON)
            .put(Entity.entity("{\"value\":\"999\"}", MediaType.APPLICATION_JSON));
    // Should fail for read-only config
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void listBrokerConfigs_differentBrokers_returnConfigs() {
    String clusterId = getClusterId();

    // Verify configs can be listed for all brokers
    for (int i = 0; i < getBrokers().size(); i++) {
      int brokerId = getBrokers().get(i).id();
      Response response =
          request("/v3/clusters/" + clusterId + "/brokers/" + brokerId + "/configs")
              .accept(MediaType.APPLICATION_JSON)
              .get();
      assertEquals(
          "Should be able to list configs for broker " + brokerId,
          Status.OK.getStatusCode(), response.getStatus());
    }
  }
}
