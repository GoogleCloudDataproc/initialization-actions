/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.hadoop.testing.EntriesCredentialConfigurationUtil;
import com.google.cloud.hadoop.testing.EntriesCredentialConfigurationUtil.TestEntries;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration.Entries;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EntriesCredentialConfigurationTest {

  private static void setConfigurationKey(
      Entries conf, String key, String value) {
    conf.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX + key, value);
  }

  private static String getConfigurationKey(Entries conf, String key) {
    return conf.get(EntriesCredentialConfiguration.BASE_KEY_PREFIX + key);
  }

  @Test
  public void componentsCanOverrideBaseConfiguration() {
    Entries configuration =
        EntriesCredentialConfigurationUtil.getTestConfiguration();
    // Overall, use service accounts
    configuration.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX
        + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
            "true");

    // In the testing prefix, disable service accounts
    configuration.set("testing."
        + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
            "false");

    configuration.set("testing."
        + EntriesCredentialConfiguration.CLIENT_ID_SUFFIX, "aClientId");
    configuration.set("testing."
        + EntriesCredentialConfiguration.CLIENT_SECRET_SUFFIX, "aClientSecret");
    configuration.set("testing."
        + EntriesCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX,
            "aCredentialFile");

    CredentialConfiguration credentialConfiguration =
        EntriesCredentialConfiguration.newEntriesBuilder()
          .withConfiguration(configuration)
          .withOverridePrefix("testing.")
          .build();

    assertEquals("aClientId", credentialConfiguration.getClientId());
    assertEquals("aClientSecret", credentialConfiguration.getClientSecret());
    assertEquals("aCredentialFile",
        credentialConfiguration.getOAuthCredentialFile());
  }

  @Test
  public void setConfiugrationSetsValuesAsExpected() {
    Entries conf = new TestEntries();

    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
        "anEmail");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
        "aKeyFile");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.CLIENT_SECRET_SUFFIX,
        "aClientSecret");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.CLIENT_ID_SUFFIX,
        "aClientId");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX,
        "aClientOAuthFile");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        "false");
    setConfigurationKey(
        conf,
        EntriesCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX,
        "true");
    conf.set(
        EntriesCredentialConfiguration.PROXY_ADDRESS_KEY,
        "foo.bar:1234");
    conf.set(
        EntriesCredentialConfiguration.HTTP_TRANSPORT_KEY,
        "APACHE");

    CredentialConfiguration credentialConfiguration =
        EntriesCredentialConfiguration
          .newEntriesBuilder()
          .withConfiguration(conf)
          .build();

    assertEquals("anEmail", credentialConfiguration.getServiceAccountEmail());
    assertEquals("aKeyFile",
        credentialConfiguration.getServiceAccountKeyFile());
    assertEquals("aClientSecret", credentialConfiguration.getClientSecret());
    assertEquals("aClientId", credentialConfiguration.getClientId());
    assertEquals("aClientOAuthFile",
        credentialConfiguration.getOAuthCredentialFile());
    assertFalse(credentialConfiguration.isServiceAccountEnabled());
    assertTrue(credentialConfiguration.isNullCredentialEnabled());
    assertEquals("foo.bar:1234", credentialConfiguration.getProxyAddress());
    assertEquals(HttpTransportType.APACHE, credentialConfiguration.getTransportType());
  }

  @Test
  public void getConfigurationSetsValuesAsAxpected() {
    List<String> prefixes =
        ImmutableList.of(EntriesCredentialConfiguration.BASE_KEY_PREFIX);

    EntriesCredentialConfiguration credentialConfiguration =
        new EntriesCredentialConfiguration(prefixes);

    credentialConfiguration.setServiceAccountEmail("anEmail");
    Entries conf = new TestEntries();
    credentialConfiguration.getConfigurationInto(conf);
    String writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX);
    assertEquals("anEmail", writtenValue);

    credentialConfiguration.setServiceAccountKeyFile("aKeyFile");
    credentialConfiguration.setProxyAddress("foo.bar:1234");
    credentialConfiguration.setTransportType(HttpTransportType.APACHE);
    credentialConfiguration.getConfigurationInto(conf);
    writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX);
    assertEquals("aKeyFile", writtenValue);

    credentialConfiguration.setClientSecret("clientSecret");
    credentialConfiguration.getConfigurationInto(conf);
    writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.CLIENT_SECRET_SUFFIX);
    assertEquals("clientSecret", writtenValue);

    credentialConfiguration.setClientId("clientId");
    credentialConfiguration.getConfigurationInto(conf);
    writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.CLIENT_ID_SUFFIX);
    assertEquals("clientId", writtenValue);

    credentialConfiguration.setEnableServiceAccounts(false);
    credentialConfiguration.getConfigurationInto(conf);
    writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX);
    assertEquals("false", writtenValue);

    credentialConfiguration.setNullCredentialEnabled(true);
    credentialConfiguration.getConfigurationInto(conf);
    writtenValue = getConfigurationKey(
        conf,
        EntriesCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX);
    assertEquals("true", writtenValue);
    assertEquals("foo.bar:1234", conf.get(EntriesCredentialConfiguration.PROXY_ADDRESS_KEY));
    assertEquals("APACHE", conf.get(EntriesCredentialConfiguration.HTTP_TRANSPORT_KEY));
  }
}
