/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.CredentialFactory.CREDENTIAL_ENV_VAR;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.CLIENT_ID_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.TOKEN_SERVER_URL_SUFFIX;
import static com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType.JAVA_NET;
import static com.google.cloud.hadoop.util.testing.HadoopConfigurationUtils.getDefaultProperties;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.cloud.hadoop.util.CredentialFactory.GoogleCredentialWithRetry;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopCredentialConfigurationTest {

  @SuppressWarnings("DoubleBraceInitialization")
  private static final Map<String, Object> expectedDefaultConfiguration =
      new HashMap<String, Object>() {
        {
          put(".auth.access.token.provider.impl", null);
          put(".auth.client.file", null);
          put(".auth.client.id", null);
          put(".auth.client.secret", null);
          put(".auth.null.enable", false);
          put(".auth.service.account.email", null);
          put(".auth.service.account.enable", true);
          put(".auth.service.account.json.keyfile", null);
          put(".auth.service.account.keyfile", null);
          put(".auth.service.account.private.key", null);
          put(".auth.service.account.private.key.id", null);
          put(".token.server.url", "https://oauth2.googleapis.com/token");
          put("fs.gs.http.transport.type", JAVA_NET);
          put("fs.gs.proxy.address", null);
          put("fs.gs.proxy.password", null);
          put("fs.gs.proxy.username", null);
        }
      };

  private static final ImmutableList<String> TEST_SCOPES = ImmutableList.of("scope1", "scope2");

  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private Configuration configuration;

  private static String getConfigKey(HadoopConfigurationProperty<?> suffixProperty) {
    return HadoopCredentialConfiguration.BASE_KEY_PREFIX + suffixProperty.getKey();
  }

  @Before
  public void setUp() {
    configuration = new Configuration();
  }

  private CredentialFactory getCredentialFactory() {
    return getCredentialFactory(new MockHttpTransport());
  }

  private CredentialFactory getCredentialFactory(HttpTransport transport) {
    CredentialFactory credentialFactory =
        HadoopCredentialConfiguration.getCredentialFactory(configuration, ImmutableList.of());
    credentialFactory.setTransport(transport);
    return credentialFactory;
  }

  @Test
  public void nullCredentialsAreCreatedForTesting() throws Exception {
    configuration.setBoolean(getConfigKey(ENABLE_SERVICE_ACCOUNTS_SUFFIX), false);
    configuration.setBoolean(getConfigKey(ENABLE_NULL_CREDENTIAL_SUFFIX), true);

    CredentialFactory credentialFactory = getCredentialFactory();

    assertThat(credentialFactory.getCredential(TEST_SCOPES)).isNull();
  }

  @Test
  public void exceptionIsThrownForNoServiceAccountEmail() {
    // No email set, keyfile doesn't exist, but that's OK.
    configuration.set(getConfigKey(SERVICE_ACCOUNT_KEYFILE_SUFFIX), "aFile");

    CredentialFactory credentialFactory = getCredentialFactory();

    assertThrows(IllegalStateException.class, () -> credentialFactory.getCredential(TEST_SCOPES));
  }

  @Test
  public void exceptionIsThrownForNoCredentialOptions() {
    configuration.setBoolean(getConfigKey(ENABLE_SERVICE_ACCOUNTS_SUFFIX), false);

    CredentialFactory credentialFactory = getCredentialFactory();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> credentialFactory.getCredential(TEST_SCOPES));
    assertThat(thrown).hasMessageThat().contains("No valid credential configuration discovered.");
  }

  @Test
  public void metadataServiceIsUsedByDefault() throws Exception {
    TokenResponse token = new TokenResponse().setAccessToken("metadata-test-token");

    MockHttpTransport transport = mockTransport(jsonDataResponse(token));
    CredentialFactory.setStaticHttpTransport(transport);

    CredentialFactory credentialFactory = getCredentialFactory();
    Credential credential = credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getAccessToken()).isEqualTo("metadata-test-token");
  }

  @Test
  public void applicationDefaultServiceAccountWhenConfigured() throws Exception {
    environmentVariables.set(
        CREDENTIAL_ENV_VAR, Resources.getResource("test-credential.json").getFile());

    CredentialFactory credentialFactory = getCredentialFactory();

    GoogleCredentialWithRetry credential =
        (GoogleCredentialWithRetry) credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getServiceAccountId()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credential.getServiceAccountPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void p12KeyFileUsedWhenConfigured() throws Exception {
    configuration.set(getConfigKey(SERVICE_ACCOUNT_EMAIL_SUFFIX), "foo@example.com");
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_KEYFILE_SUFFIX),
        Resources.getResource("test-key.p12").getFile());

    CredentialFactory credentialFactory = getCredentialFactory();

    GoogleCredentialWithRetry credential =
        (GoogleCredentialWithRetry) credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getServiceAccountId()).isEqualTo("foo@example.com");
  }

  @Test
  public void jsonKeyFileUsedWhenConfigured() throws Exception {
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX),
        Resources.getResource("test-credential.json").getFile());

    CredentialFactory credentialFactory = getCredentialFactory();

    GoogleCredentialWithRetry credential =
        (GoogleCredentialWithRetry) credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getServiceAccountId()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(credential.getServiceAccountPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  public void configurationSAUsedWhenConfigured() throws Exception {
    configuration.set(getConfigKey(SERVICE_ACCOUNT_EMAIL_SUFFIX), "foo@example.com");
    configuration.set(getConfigKey(SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX), "privatekey");
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX),
        Resources.toString(Resources.getResource("test-key.txt"), StandardCharsets.UTF_8));

    CredentialFactory credentialFactory = getCredentialFactory();

    GoogleCredentialWithRetry credential =
        (GoogleCredentialWithRetry) credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getServiceAccountId()).isEqualTo("foo@example.com");
    assertThat(credential.getServiceAccountPrivateKeyId()).isEqualTo("privatekey");
  }

  @Test
  public void installedAppWorkflowUsedWhenConfigured() throws Exception {
    configuration.setBoolean(getConfigKey(ENABLE_SERVICE_ACCOUNTS_SUFFIX), false);
    configuration.set(getConfigKey(CLIENT_ID_SUFFIX), "aClientId");
    configuration.set(getConfigKey(CLIENT_SECRET_SUFFIX), "aClientSecret");
    configuration.set(
        getConfigKey(OAUTH_CLIENT_FILE_SUFFIX),
        Resources.getResource("test-client-credential.json").getFile());

    CredentialFactory credentialFactory = getCredentialFactory();

    Credential credential = credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getAccessToken()).isEqualTo("test-client-access-token");
    assertThat(credential.getRefreshToken()).isEqualTo("test-client-refresh-token");
  }

  @Test
  public void customTokenServerUrl() throws Exception {
    configuration.set(
        getConfigKey(SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX),
        Resources.getResource("test-credential.json").getFile());
    configuration.set(getConfigKey(TOKEN_SERVER_URL_SUFFIX), "https://test.oauth.com/token");

    CredentialFactory credentialFactory = getCredentialFactory();

    GoogleCredentialWithRetry credential =
        (GoogleCredentialWithRetry) credentialFactory.getCredential(TEST_SCOPES);

    assertThat(credential.getTokenServerEncodedUrl()).isEqualTo("https://test.oauth.com/token");
  }

  @Test
  public void defaultPropertiesValues() {
    assertThat(getDefaultProperties(HadoopCredentialConfiguration.class))
        .containsExactlyEntriesIn(expectedDefaultConfiguration);
  }
}
