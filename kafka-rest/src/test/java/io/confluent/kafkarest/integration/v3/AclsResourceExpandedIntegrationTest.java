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

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.v3.AclData;
import io.confluent.kafkarest.entities.v3.AclDataList;
import io.confluent.kafkarest.entities.v3.SearchAclsResponse;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AclsResourceExpandedIntegrationTest extends ClusterTestHarness {

  public AclsResourceExpandedIntegrationTest() {
    super(/* numBrokers= */ 3, /* withSchemaRegistry= */ false);
  }

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @Override
  public Properties overrideBrokerProperties(int i, Properties props) {
    props.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
    props.put(
        "listener.name.sasl_plaintext.plain.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"kafka\" "
            + "password=\"kafka\" "
            + "user_kafka=\"kafka\" "
            + "user_kafkarest=\"kafkarest\" "
            + "user_alice=\"alice\" "
            + "user_bob=\"bob\";");
    props.put("sasl.enabled.mechanisms", "PLAIN");
    props.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
    props.put("super.users", "User:kafka;User:kafkarest");
    return props;
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        "io.confluent.kafkarest.security.KafkaRestSecurityResourceExtension");
    restProperties.put("confluent.rest.auth.ssl.principal.mapping.rules", "DEFAULT");
    restProperties.put(
        "kafka.rest.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"kafkarest\" "
            + "password=\"kafkarest\";");
    restProperties.put("kafka.rest.sasl.mechanism", "PLAIN");
  }

  @Test
  public void createAcl_forGroupResource_createsSuccessfully() {
    String clusterId = getClusterId();

    // Create an ACL for a consumer GROUP resource
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"resource_type\":\"GROUP\","
                        + "\"resource_name\":\"my-consumer-group\","
                        + "\"pattern_type\":\"LITERAL\","
                        + "\"principal\":\"User:alice\","
                        + "\"host\":\"*\","
                        + "\"operation\":\"READ\","
                        + "\"permission\":\"ALLOW\"}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), createResponse.getStatus());

    // Search for the created ACL
    testWithRetry(
        () -> {
          Response searchResponse =
              request(
                  "/v3/clusters/" + clusterId + "/acls"
                      + "?resource_type=GROUP"
                      + "&resource_name=my-consumer-group"
                      + "&pattern_type=LITERAL"
                      + "&principal=User:alice"
                      + "&host=*"
                      + "&operation=READ"
                      + "&permission=ALLOW")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), searchResponse.getStatus());

          SearchAclsResponse actual = searchResponse.readEntity(SearchAclsResponse.class);
          assertEquals(1, actual.getValue().getData().size());

          AclData acl = actual.getValue().getData().get(0);
          assertEquals("GROUP", acl.getResourceType().toString());
          assertEquals("my-consumer-group", acl.getResourceName());
          assertEquals("User:alice", acl.getPrincipal());
        });

    // Clean up: delete the ACL
    Response deleteResponse =
        request(
            "/v3/clusters/" + clusterId + "/acls"
                + "?resource_type=GROUP"
                + "&resource_name=my-consumer-group"
                + "&pattern_type=LITERAL"
                + "&principal=User:alice"
                + "&host=*"
                + "&operation=READ"
                + "&permission=ALLOW")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.OK.getStatusCode(), deleteResponse.getStatus());
  }

  @Test
  public void createAcl_forClusterResource_createsSuccessfully() {
    String clusterId = getClusterId();

    // Create an ACL for CLUSTER resource type
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"resource_type\":\"CLUSTER\","
                        + "\"resource_name\":\"kafka-cluster\","
                        + "\"pattern_type\":\"LITERAL\","
                        + "\"principal\":\"User:bob\","
                        + "\"host\":\"*\","
                        + "\"operation\":\"ALTER\","
                        + "\"permission\":\"ALLOW\"}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), createResponse.getStatus());

    // Verify
    testWithRetry(
        () -> {
          Response searchResponse =
              request(
                  "/v3/clusters/" + clusterId + "/acls"
                      + "?resource_type=CLUSTER"
                      + "&resource_name=kafka-cluster"
                      + "&pattern_type=LITERAL"
                      + "&principal=User:bob")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), searchResponse.getStatus());

          SearchAclsResponse actual = searchResponse.readEntity(SearchAclsResponse.class);
          assertTrue(actual.getValue().getData().size() > 0);
        });

    // Clean up
    request(
        "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=CLUSTER"
            + "&resource_name=kafka-cluster"
            + "&pattern_type=LITERAL"
            + "&principal=User:bob"
            + "&host=*"
            + "&operation=ALTER"
            + "&permission=ALLOW")
        .accept(MediaType.APPLICATION_JSON)
        .delete();
  }

  @Test
  public void deleteAcls_multipleMatchingAcls_deletesAll() {
    String clusterId = getClusterId();

    // Create multiple ACLs for the same principal and resource
    request("/v3/clusters/" + clusterId + "/acls")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"resource_type\":\"TOPIC\","
                    + "\"resource_name\":\"multi-delete-topic\","
                    + "\"pattern_type\":\"LITERAL\","
                    + "\"principal\":\"User:alice\","
                    + "\"host\":\"*\","
                    + "\"operation\":\"READ\","
                    + "\"permission\":\"ALLOW\"}",
                MediaType.APPLICATION_JSON));

    request("/v3/clusters/" + clusterId + "/acls")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"resource_type\":\"TOPIC\","
                    + "\"resource_name\":\"multi-delete-topic\","
                    + "\"pattern_type\":\"LITERAL\","
                    + "\"principal\":\"User:alice\","
                    + "\"host\":\"*\","
                    + "\"operation\":\"WRITE\","
                    + "\"permission\":\"ALLOW\"}",
                MediaType.APPLICATION_JSON));

    request("/v3/clusters/" + clusterId + "/acls")
        .accept(MediaType.APPLICATION_JSON)
        .post(
            Entity.entity(
                "{\"resource_type\":\"TOPIC\","
                    + "\"resource_name\":\"multi-delete-topic\","
                    + "\"pattern_type\":\"LITERAL\","
                    + "\"principal\":\"User:alice\","
                    + "\"host\":\"*\","
                    + "\"operation\":\"DESCRIBE\","
                    + "\"permission\":\"ALLOW\"}",
                MediaType.APPLICATION_JSON));

    // Verify all 3 ACLs were created
    testWithRetry(
        () -> {
          Response searchResponse =
              request(
                  "/v3/clusters/" + clusterId + "/acls"
                      + "?resource_type=TOPIC"
                      + "&resource_name=multi-delete-topic"
                      + "&pattern_type=LITERAL"
                      + "&principal=User:alice")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), searchResponse.getStatus());

          SearchAclsResponse actual = searchResponse.readEntity(SearchAclsResponse.class);
          assertEquals(3, actual.getValue().getData().size());
        });

    // Delete all ACLs for this topic and principal at once
    Response deleteResponse =
        request(
            "/v3/clusters/" + clusterId + "/acls"
                + "?resource_type=TOPIC"
                + "&resource_name=multi-delete-topic"
                + "&pattern_type=LITERAL"
                + "&principal=User:alice"
                + "&host=*"
                + "&permission=ALLOW")
            .accept(MediaType.APPLICATION_JSON)
            .delete();
    assertEquals(Status.OK.getStatusCode(), deleteResponse.getStatus());

    // Verify all ACLs were deleted
    testWithRetry(
        () -> {
          Response searchAfterDelete =
              request(
                  "/v3/clusters/" + clusterId + "/acls"
                      + "?resource_type=TOPIC"
                      + "&resource_name=multi-delete-topic"
                      + "&pattern_type=LITERAL"
                      + "&principal=User:alice")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), searchAfterDelete.getStatus());

          SearchAclsResponse actual = searchAfterDelete.readEntity(SearchAclsResponse.class);
          assertEquals(0, actual.getValue().getData().size());
        });
  }

  @Test
  public void createAcl_prefixedPattern_createsSuccessfully() {
    String clusterId = getClusterId();

    // Create an ACL with PREFIXED pattern type
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"resource_type\":\"TOPIC\","
                        + "\"resource_name\":\"prefix-test-\","
                        + "\"pattern_type\":\"PREFIXED\","
                        + "\"principal\":\"User:bob\","
                        + "\"host\":\"*\","
                        + "\"operation\":\"READ\","
                        + "\"permission\":\"ALLOW\"}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), createResponse.getStatus());

    // Clean up
    request(
        "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=TOPIC"
            + "&resource_name=prefix-test-"
            + "&pattern_type=PREFIXED"
            + "&principal=User:bob"
            + "&host=*"
            + "&operation=READ"
            + "&permission=ALLOW")
        .accept(MediaType.APPLICATION_JSON)
        .delete();
  }

  @Test
  public void createAcl_denyPermission_createsSuccessfully() {
    String clusterId = getClusterId();

    // Create a DENY ACL
    Response createResponse =
        request("/v3/clusters/" + clusterId + "/acls")
            .accept(MediaType.APPLICATION_JSON)
            .post(
                Entity.entity(
                    "{\"resource_type\":\"TOPIC\","
                        + "\"resource_name\":\"deny-test-topic\","
                        + "\"pattern_type\":\"LITERAL\","
                        + "\"principal\":\"User:alice\","
                        + "\"host\":\"*\","
                        + "\"operation\":\"WRITE\","
                        + "\"permission\":\"DENY\"}",
                    MediaType.APPLICATION_JSON));
    assertEquals(Status.CREATED.getStatusCode(), createResponse.getStatus());

    // Verify
    testWithRetry(
        () -> {
          Response searchResponse =
              request(
                  "/v3/clusters/" + clusterId + "/acls"
                      + "?resource_type=TOPIC"
                      + "&resource_name=deny-test-topic"
                      + "&pattern_type=LITERAL"
                      + "&principal=User:alice"
                      + "&permission=DENY")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), searchResponse.getStatus());

          SearchAclsResponse actual = searchResponse.readEntity(SearchAclsResponse.class);
          assertEquals(1, actual.getValue().getData().size());
          assertEquals("DENY", actual.getValue().getData().get(0).getPermission().toString());
        });

    // Clean up
    request(
        "/v3/clusters/" + clusterId + "/acls"
            + "?resource_type=TOPIC"
            + "&resource_name=deny-test-topic"
            + "&pattern_type=LITERAL"
            + "&principal=User:alice"
            + "&host=*"
            + "&operation=WRITE"
            + "&permission=DENY")
        .accept(MediaType.APPLICATION_JSON)
        .delete();
  }

  @Test
  public void searchAcls_nonExistingCluster_returnsNotFound() {
    Response response =
        request(
            "/v3/clusters/foobar/acls"
                + "?resource_type=TOPIC"
                + "&resource_name=test"
                + "&pattern_type=LITERAL")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
