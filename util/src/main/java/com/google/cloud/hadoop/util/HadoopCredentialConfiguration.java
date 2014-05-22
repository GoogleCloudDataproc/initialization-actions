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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * CredentialConfiguration based on Hadoop Configuration objects.
 *
 * When reading configuration this class makes use of a list of key prefixes that are each applied
 * to key suffixes to create a complete configuration key. There is a base prefix of 'google.cloud.'
 * that is included by the builder for each HadoopCredentialConfiguration created. When
 * constructing, other prefixes can be specified. Prefixes specified later can be used to override
 * the values of previously set values. In this way a set of global credentials can be specified
 * for most connectors with an override specified for any connectors that need different
 * credentials.
 */
public class HadoopCredentialConfiguration extends CredentialConfiguration implements Configurable {

  /**
   * All instances constructed using the builder will use "google.cloud" as the first
   * prefix checked. Other prefixes can be added and will override values in the google.cloud
   * prefix.
   */
  public static final String BASE_KEY_PREFIX = "google.cloud";
  /**
   * Key suffix used to disable service accounts. A value of 'false' will disable the use of
   * service accounts. The default is to use a service account.
   */
  public static final String ENABLE_SERVICE_ACCOUNTS_SUFFIX = ".auth.service.account.enable";
  /**
   * Key suffix used to control which email address is associated with the service account.
   */
  public static final String SERVICE_ACCOUNT_EMAIL_SUFFIX = ".auth.service.account.email";
  /**
   * Key suffix used to indicate the path to the service account p12 keyfile. If provided, triggers
   * private keyfile service account authentication. The file will be required to be present on all
   * nodes and at the same location on all nodes.
   */
  public static final String SERVICE_ACCOUNT_KEYFILE_SUFFIX = ".auth.service.account.keyfile";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client ID for
   * the credentials.
   */
  public static final String CLIENT_ID_SUFFIX = ".auth.client.id";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client secret
   * for the credentials.
   */
  public static final String CLIENT_SECRET_SUFFIX = ".auth.client.secret";
  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the file containing
   * credentials (JWT).
   */
  public static final String OAUTH_CLIENT_FILE_SUFFIX = ".auth.client.file";
  /**
   * For unit-testing, the key suffix allowing null to be returned from credential creation
   * instead of logging an error and aborting.
   */
  public static final String ENABLE_NULL_CREDENTIAL_SUFFIX = ".auth.null.enable";

  /**
   * Builder for constructing CredentialConfiguration instances.
   */
  public static class Builder {
    private Configuration configuration;
    private List<String> prefixes = new ArrayList<>();
    private CredentialFactory credentialFactory = new CredentialFactory();

    public Builder () {
      prefixes.add(BASE_KEY_PREFIX);
    }

    @VisibleForTesting
    Builder withCredentialFactory(CredentialFactory credentialFactory) {
      this.credentialFactory = credentialFactory;
      return this;
    }

    public Builder withConfiguration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    public Builder withOverridePrefix(String prefix) {
      this.prefixes.add(prefix);
      return this;
    }

    public CredentialConfiguration build() {
      HadoopCredentialConfiguration credentialConfiguration =
          new HadoopCredentialConfiguration(prefixes);
      if (configuration != null) {
        credentialConfiguration.setConf(configuration);
      }
      if (credentialFactory != null) {
        credentialConfiguration.setCredentialFactory(credentialFactory);
      }
      return credentialConfiguration;
    }
  }

  /**
   * A builder for setting up CredentialConfiguration objects.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private static Optional<Boolean> maybeGetBoolean(Configuration config, String key) {
    String value = config.get(key);
    if (value == null) {
      return Optional.absent();
    }
    return Optional.of(Boolean.valueOf(value));
  }

  private final List<String> prefixes;
  public HadoopCredentialConfiguration(List<String> prefixes) {
    this.prefixes = ImmutableList.copyOf(prefixes);
  }

  public Configuration getConf() {
    Configuration configuration = new Configuration();
    for (String prefix : prefixes) {
      configuration.setBoolean(prefix + ENABLE_SERVICE_ACCOUNTS_SUFFIX, isServiceAccountEnabled());

      if (getServiceAccountEmail() != null) {
        configuration.set(prefix + SERVICE_ACCOUNT_EMAIL_SUFFIX, getServiceAccountEmail());
      }
      if (getServiceAccountKeyFile() != null) {
        configuration.set(prefix + SERVICE_ACCOUNT_KEYFILE_SUFFIX, getServiceAccountKeyFile());
      }
      if (getClientId() != null) {
        configuration.set(prefix + CLIENT_ID_SUFFIX, getClientId());
      }
      if (getClientSecret() != null) {
        configuration.set(prefix + CLIENT_SECRET_SUFFIX, getClientSecret());
      }
      if (getOAuthCredentialFile() != null) {
        configuration.set(prefix + OAUTH_CLIENT_FILE_SUFFIX, getOAuthCredentialFile());
      }

      configuration.setBoolean(prefix + ENABLE_NULL_CREDENTIAL_SUFFIX, isNullCredentialEnabled());
    }
    return configuration;
  }

  /**
   * Load configuration values from the provided Configuration source. For any key that does not
   * have a corresponding value in the Configuration, no changes will be made to the state of this
   * object.
   */
  public void setConf(Configuration entries) {
    for (String prefix : prefixes) {
      Optional<Boolean> enableServiceAccounts =
          maybeGetBoolean(entries, prefix + ENABLE_SERVICE_ACCOUNTS_SUFFIX);

      if (enableServiceAccounts.isPresent()) {
        setEnableServiceAccounts(enableServiceAccounts.get());
      }

      String serviceEmailAccount = entries.get(prefix + SERVICE_ACCOUNT_EMAIL_SUFFIX);
      if (serviceEmailAccount != null) {
        setServiceAccountEmail(serviceEmailAccount);
      }

      String serviceAccountKeyFile = entries.get(prefix + SERVICE_ACCOUNT_KEYFILE_SUFFIX);
      if (serviceAccountKeyFile != null) {
        setServiceAccountKeyFile(serviceAccountKeyFile);
      }

      String clientId = entries.get(prefix + CLIENT_ID_SUFFIX);
      if (clientId != null) {
        setClientId(clientId);
      }

      String clientSecret = entries.get(prefix + CLIENT_SECRET_SUFFIX);
      if (clientSecret != null) {
        setClientSecret(clientSecret);
      }

      String oAuthCredentialPath = entries.get(prefix + OAUTH_CLIENT_FILE_SUFFIX);
      if (oAuthCredentialPath != null) {
        setOAuthCredentialFile(oAuthCredentialPath);
      }

      Optional<Boolean> enableNullCredential = maybeGetBoolean(entries,
          prefix + ENABLE_NULL_CREDENTIAL_SUFFIX);
      if (enableNullCredential.isPresent()) {
        setNullCredentialEnabled(enableNullCredential.get());
      }
    }
  }
}
