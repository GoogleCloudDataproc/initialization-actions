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

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class HadoopCredentialConfigurationTest {

  private static void setConfigurationKey(Configuration conf, String key, String value) {
    conf.set(HadoopCredentialConfiguration.BASE_KEY_PREFIX + key, value);
  }

  private static String getConfigurationKey(Configuration conf, String key) {
    return conf.get(HadoopCredentialConfiguration.BASE_KEY_PREFIX + key);
  }

  @Test
  public void componentsCanOverrideBaseConfiguration() {
    Configuration configuration = new Configuration();
    // Overall, use service accounts
    configuration.set(HadoopCredentialConfiguration.BASE_KEY_PREFIX +
        HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX, "true");

    // In the testing prefix, disable service accounts
    configuration.set("testing." +
        HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX, "false");

    configuration.set("testing." +
        HadoopCredentialConfiguration.CLIENT_ID_SUFFIX, "aClientId");
    configuration.set("testing." +
        HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX, "aClientSecret");
    configuration.set("testing." +
        HadoopCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX, "aCredentialFile");

    CredentialConfiguration credentialConfiguration = HadoopCredentialConfiguration.newBuilder()
        .withConfiguration(configuration)
        .withOverridePrefix("testing.")
        .build();

    assertEquals("aClientId", credentialConfiguration.getClientId());
    assertEquals("aClientSecret", credentialConfiguration.getClientSecret());
    assertEquals("aCredentialFile", credentialConfiguration.getOAuthCredentialFile());
  }

  @Test
  public void setConfiugrationSetsValuesAsExpected() {
    Configuration conf = new Configuration();

    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
        "anEmail");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
        "aKeyFile");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX,
        "aClientSecret");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.CLIENT_ID_SUFFIX,
        "aClientId");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX,
        "aClientOAuthFile");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        "false");
    setConfigurationKey(
        conf,
        HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX,
        "true");

    CredentialConfiguration credentialConfiguration = HadoopCredentialConfiguration
        .newBuilder()
        .withConfiguration(conf)
        .build();

    assertEquals("anEmail", credentialConfiguration.getServiceAccountEmail());
    assertEquals("aKeyFile", credentialConfiguration.getServiceAccountKeyFile());
    assertEquals("aClientSecret", credentialConfiguration.getClientSecret());
    assertEquals("aClientId", credentialConfiguration.getClientId());
    assertEquals("aClientOAuthFile", credentialConfiguration.getOAuthCredentialFile());
    assertFalse(credentialConfiguration.isServiceAccountEnabled());
    assertTrue(credentialConfiguration.isNullCredentialEnabled());
  }

  @Test
  public void getConfigurationSetsValuesAsAxpected() {
    List<String> prefixes = ImmutableList.of(HadoopCredentialConfiguration.BASE_KEY_PREFIX);

    HadoopCredentialConfiguration credentialConfiguration =
        new HadoopCredentialConfiguration(prefixes);

    credentialConfiguration.setServiceAccountEmail("anEmail");
    Configuration conf = credentialConfiguration.getConf();
    String writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX);
    assertEquals("anEmail", writtenValue);

    credentialConfiguration.setServiceAccountKeyFile("aKeyFile");
    conf = credentialConfiguration.getConf();
    writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX);
    assertEquals("aKeyFile", writtenValue);

    credentialConfiguration.setClientSecret("clientSecret");
    conf = credentialConfiguration.getConf();
    writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX);
    assertEquals("clientSecret", writtenValue);

    credentialConfiguration.setClientId("clientId");
    conf = credentialConfiguration.getConf();
    writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.CLIENT_ID_SUFFIX);
    assertEquals("clientId", writtenValue);

    credentialConfiguration.setEnableServiceAccounts(false);
    conf = credentialConfiguration.getConf();
    writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX);
    assertEquals("false", writtenValue);

    credentialConfiguration.setNullCredentialEnabled(true);
    conf = credentialConfiguration.getConf();
    writtenValue = getConfigurationKey(
        conf,
        HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX);
    assertEquals("true", writtenValue);
  }
}
