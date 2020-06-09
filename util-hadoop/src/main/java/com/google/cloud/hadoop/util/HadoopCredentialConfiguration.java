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

import static com.google.common.base.StandardSystemProperty.USER_HOME;

import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Hadoop credential configuration.
 *
 * <p>When reading configuration this class makes use of a list of key prefixes that are each
 * applied to key suffixes to create a complete configuration key. There is a base prefix of
 * 'google.cloud.' that is included by the builder for each EntriesCredentialConfiguration created.
 * When constructing, other prefixes can be specified. Prefixes specified later can be used to
 * override the values of previously set values. In this way a set of global credentials can be
 * specified for most connectors with an override specified for any connectors that need different
 * credentials.
 */
public class HadoopCredentialConfiguration {

  /**
   * All instances constructed using the builder will use {@code google.cloud} as the first prefix
   * checked. Other prefixes can be added and will override values in the google.cloud prefix.
   */
  public static final String BASE_KEY_PREFIX = "google.cloud";

  /**
   * Key suffix for enabling GCE service account authentication. A value of {@code false} will
   * disable the use of the service accounts for authentication. The default value is {@code true} -
   * use a service account for authentication.
   */
  public static final HadoopConfigurationProperty<Boolean> ENABLE_SERVICE_ACCOUNTS_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.service.account.enable",
          CredentialOptions.SERVICE_ACCOUNT_ENABLED_DEFAULT,
          ".enable.service.account.auth");

  /**
   * Key suffix specifying the email address associated with the service account with which to
   * authenticate. Only required if {@link #ENABLE_SERVICE_ACCOUNTS_SUFFIX} is {@code true} and
   * we're using {@link #SERVICE_ACCOUNT_KEYFILE_SUFFIX} to authenticate with a private keyfile.
   */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_EMAIL_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.service.account.email", /* defaultValue= */ null, ".service.account.auth.email");

  /** Key suffix used to specify private key id for the service account. */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.service.account.private.key.id");

  /** Key suffix used to specify private key for the service account. */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.service.account.private.key");

  /**
   * Key suffix specifying local file containing a service account private {@code .p12} keyfile.
   * Only used if {@link #SERVICE_ACCOUNT_EMAIL_SUFFIX} is {@code true}; if provided, triggers
   * private keyfile service account authentication. The file will be required to be present on all
   * nodes and at the same location on all nodes.
   */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_KEYFILE_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.service.account.keyfile",
          /* defaultValue= */ null,
          ".service.account.auth.keyfile");

  /**
   * Key suffix used to indicate the path to a JSON file containing a Service Account key and
   * identifier (email). Technically, this could be a JSON containing a non-service account user,
   * but this setting is only used in the service account flow and is namespaced as such.
   */
  public static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.service.account.json.keyfile");

  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client ID for the
   * credentials.
   */
  public static final HadoopConfigurationProperty<String> CLIENT_ID_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.client.id", /* defaultValue= */ null, ".client.id");

  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the client secret for
   * the credentials.
   */
  public static final HadoopConfigurationProperty<String> CLIENT_SECRET_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.client.secret", /* defaultValue= */ null, ".client.secret");

  /**
   * For OAuth-based Installed App authentication, the key suffix specifying the file containing
   * credentials (JWT). By default we can set this fairly safely (it's only invoked if client ID,
   * client secret are set and we're not using service accounts).
   */
  public static final HadoopConfigurationProperty<String> OAUTH_CLIENT_FILE_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.client.file", USER_HOME.value() + "/.credentials/storage.json");

  /**
   * For unit-testing, the key suffix allowing null to be returned from credential creation instead
   * of logging an error and aborting.
   */
  public static final HadoopConfigurationProperty<Boolean> ENABLE_NULL_CREDENTIAL_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".auth.null.enable", CredentialOptions.NULL_CREDENTIALS_ENABLED_DEFAULT);

  /** Configuration key for setting a token server URL to use to refresh OAuth token. */
  public static final HadoopConfigurationProperty<String> TOKEN_SERVER_URL_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".token.server.url", CredentialOptions.TOKEN_SERVER_URL_DEFAULT);

  /**
   * Configuration key for setting a proxy for the connector to use to connect to GCS. The proxy
   * must be an HTTP proxy of the form "host:port".
   */
  public static final HadoopConfigurationProperty<String> PROXY_ADDRESS_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.address");

  /**
   * Configuration key for setting a proxy username for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> PROXY_USERNAME_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.username");

  /**
   * Configuration key for setting a proxy password for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> PROXY_PASSWORD_SUFFIX =
      new HadoopConfigurationProperty<>(".proxy.password");

  /**
   * Configuration key for the name of HttpTransport class to use for connecting to GCS. Must be the
   * name of an HttpTransportFactory.HttpTransportType (APACHE or JAVA_NET).
   */
  public static final HadoopConfigurationProperty<HttpTransportType> HTTP_TRANSPORT_SUFFIX =
      new HadoopConfigurationProperty<>(
          ".http.transport.type", CredentialOptions.HTTP_TRANSPORT_TYPE_DEFAULT);

  public static final HadoopConfigurationProperty<Class<? extends AccessTokenProvider>>
      ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.access.token.provider.impl");

  /**
   * Key suffix specifying the impersonating service account with which to call GCS API to get
   * access token.
   */
  public static final HadoopConfigurationProperty<String> IMPERSONATION_SERVICE_ACCOUNT_SUFFIX =
      new HadoopConfigurationProperty<>(".auth.impersonation.service.account");

  public static CredentialFactory getCredentialFactory(
      Configuration config, String... keyPrefixesVararg) {
    List<String> keyPrefixes = getConfigKeyPrefixes(keyPrefixesVararg);
    CredentialOptions credentialOptions =
        CredentialOptions.builder()
            .setServiceAccountEnabled(
                ENABLE_SERVICE_ACCOUNTS_SUFFIX
                    .withPrefixes(keyPrefixes)
                    .get(config, config::getBoolean))
            .setServiceAccountPrivateKeyId(
                RedactedString.create(
                    SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX
                        .withPrefixes(keyPrefixes)
                        .getPassword(config)))
            .setServiceAccountPrivateKey(
                RedactedString.create(
                    SERVICE_ACCOUNT_PRIVATE_KEY_SUFFIX
                        .withPrefixes(keyPrefixes)
                        .getPassword(config)))
            .setServiceAccountEmail(
                SERVICE_ACCOUNT_EMAIL_SUFFIX.withPrefixes(keyPrefixes).getPassword(config))
            .setServiceAccountKeyFile(
                SERVICE_ACCOUNT_KEYFILE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
            .setServiceAccountJsonKeyFile(
                SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX
                    .withPrefixes(keyPrefixes)
                    .get(config, config::get))
            .setClientId(
                RedactedString.create(
                    CLIENT_ID_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get)))
            .setClientSecret(
                RedactedString.create(
                    CLIENT_SECRET_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get)))
            .setOAuthCredentialFile(
                OAUTH_CLIENT_FILE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
            .setNullCredentialEnabled(
                ENABLE_NULL_CREDENTIAL_SUFFIX
                    .withPrefixes(keyPrefixes)
                    .get(config, config::getBoolean))
            .setTransportType(
                HTTP_TRANSPORT_SUFFIX.withPrefixes(keyPrefixes).get(config, config::getEnum))
            .setTokenServerUrl(
                TOKEN_SERVER_URL_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
            .setProxyAddress(
                PROXY_ADDRESS_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get))
            .setProxyUsername(
                RedactedString.create(
                    PROXY_USERNAME_SUFFIX.withPrefixes(keyPrefixes).getPassword(config)))
            .setProxyPassword(
                RedactedString.create(
                    PROXY_PASSWORD_SUFFIX.withPrefixes(keyPrefixes).getPassword(config)))
            .build();
    return new CredentialFactory(credentialOptions);
  }

  public static Class<? extends AccessTokenProvider> getAccessTokenProviderImplClass(
      Configuration config, String... keyPrefixes) {
    return ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX
        .withPrefixes(getConfigKeyPrefixes(keyPrefixes))
        .get(config, (k, d) -> config.getClass(k, d, AccessTokenProvider.class));
  }

  /**
   * Returns full list of config prefixes that will be resolved based on the order in returned list.
   */
  public static ImmutableList<String> getConfigKeyPrefixes(String... keyPrefixes) {
    return ImmutableList.<String>builder().add(keyPrefixes).add(BASE_KEY_PREFIX).build();
  }

  protected HadoopCredentialConfiguration() {}
}
