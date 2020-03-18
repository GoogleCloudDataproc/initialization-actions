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

import static com.google.cloud.hadoop.util.HttpTransportFactory.toSecretString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.googleapis.auth.oauth2.GoogleOAuthConstants;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import javax.annotation.Nullable;

/** Configuration for how components should obtain Credentials. */
@AutoValue
public abstract class CredentialOptions {

  static final boolean SERVICE_ACCOUNT_ENABLED_DEFAULT = true;

  static final boolean NULL_CREDENTIALS_ENABLED_DEFAULT = false;

  static final HttpTransportType HTTP_TRANSPORT_TYPE_DEFAULT =
      HttpTransportFactory.DEFAULT_TRANSPORT_TYPE;

  static final String TOKEN_SERVER_URL_DEFAULT = GoogleOAuthConstants.TOKEN_SERVER_URL;

  public static Builder builder() {
    return new AutoValue_CredentialOptions.Builder()
        .setServiceAccountEnabled(SERVICE_ACCOUNT_ENABLED_DEFAULT)
        .setNullCredentialEnabled(NULL_CREDENTIALS_ENABLED_DEFAULT)
        .setTransportType(HTTP_TRANSPORT_TYPE_DEFAULT)
        .setTokenServerUrl(TOKEN_SERVER_URL_DEFAULT);
  }

  public abstract Builder toBuilder();

  public abstract boolean isServiceAccountEnabled();

  // The following 2 parameters are used for credentials set directly via Hadoop Configuration

  @Nullable
  public abstract String getServiceAccountPrivateKeyId();

  @Nullable
  public abstract String getServiceAccountPrivateKey();

  // The following 2 parameters are used for ServiceAccount P12 KeyFiles

  @Nullable
  public abstract String getServiceAccountEmail();

  @Nullable
  public abstract String getServiceAccountKeyFile();

  // The following parameter is used for ServiceAccount Json KeyFiles

  @Nullable
  public abstract String getServiceAccountJsonKeyFile();

  // The following 3 parameters are used for client authentication

  @Nullable
  public abstract String getClientId();

  @Nullable
  public abstract String getClientSecret();

  @Nullable
  public abstract String getOAuthCredentialFile();

  public abstract boolean isNullCredentialEnabled();

  public abstract HttpTransportType getTransportType();

  public abstract String getTokenServerUrl();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract String getProxyUsername();

  @Nullable
  public abstract String getProxyPassword();

  @Override
  public final String toString() {
    return "CredentialOptions{\n"
        + ("serviceAccountEnabled: " + isServiceAccountEnabled() + '\n')
        + ("serviceAccountPrivateKeyId: " + toSecretString(getServiceAccountPrivateKeyId()) + '\n')
        + ("serviceAccountPrivateKey: " + toSecretString(getServiceAccountPrivateKey()) + '\n')
        + ("serviceAccountEmail: " + getServiceAccountEmail() + '\n')
        + ("serviceAccountKeyfile: " + getServiceAccountKeyFile() + '\n')
        + ("serviceAccountJsonKeyFile: " + getServiceAccountJsonKeyFile() + '\n')
        + ("clientId: " + toSecretString(getClientId()) + '\n')
        + ("clientSecret: " + toSecretString(getClientSecret()) + '\n')
        + ("oAuthCredentialFile: " + getOAuthCredentialFile() + '\n')
        + ("nullCredentialEnabled: " + isNullCredentialEnabled() + '\n')
        + ("transportType: " + getTransportType() + '\n')
        + ("tokenServerUrl: " + getTokenServerUrl() + '\n')
        + ("proxyAddress: " + getProxyAddress() + '\n')
        + ("proxyUsername: " + toSecretString(getProxyUsername()) + '\n')
        + ("proxyPassword: " + toSecretString(getProxyPassword()) + '\n')
        + "}";
  }

  /** Builder for {@link CredentialOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setServiceAccountEnabled(boolean value);

    public abstract Builder setServiceAccountPrivateKeyId(String serviceAccountPrivateKeyId);

    public abstract Builder setServiceAccountPrivateKey(String serviceAccountPrivateKey);

    public abstract Builder setServiceAccountEmail(String serviceAccountEmail);

    public abstract Builder setServiceAccountKeyFile(String serviceAccountKeyFile);

    public abstract Builder setServiceAccountJsonKeyFile(String serviceAccountJsonKeyFile);

    public abstract Builder setClientId(String clientId);

    public abstract Builder setClientSecret(String clientSecret);

    public abstract Builder setOAuthCredentialFile(String oAuthCredentialFile);

    public abstract Builder setNullCredentialEnabled(boolean nullCredentialEnabled);

    public abstract Builder setTransportType(HttpTransportType transportType);

    public abstract Builder setTokenServerUrl(String tokenServerUrl);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(String proxyUsername);

    public abstract Builder setProxyPassword(String proxyPassword);

    abstract CredentialOptions autoBuild();

    public CredentialOptions build() {
      CredentialOptions options = autoBuild();

      if (options.isServiceAccountEnabled()) {
        if (!isNullOrEmpty(options.getServiceAccountPrivateKeyId())) {
          checkArgument(
              !isNullOrEmpty(options.getServiceAccountPrivateKey()),
              "privateKeyId must be set if using credentials configured directly in"
                  + " configuration.");
          checkArgument(
              !isNullOrEmpty(options.getServiceAccountEmail()),
              "clientEmail must be set if using credentials configured directly in configuration.");

          checkArgument(
              isNullOrEmpty(options.getServiceAccountKeyFile()),
              "A P12 key file may not be specified at the same time as credentials"
                  + " via configuration.");
          checkArgument(
              isNullOrEmpty(options.getServiceAccountJsonKeyFile()),
              "A JSON key file may not be specified at the same time as credentials"
                  + " via configuration.");
        }

        if (!isNullOrEmpty(options.getServiceAccountJsonKeyFile())) {
          checkArgument(
              isNullOrEmpty(options.getServiceAccountKeyFile()),
              "A P12 key file may not be specified at the same time as a JSON key file.");
          checkArgument(
              isNullOrEmpty(options.getServiceAccountEmail()),
              "Service account email may not be specified at the same time as a JSON key file.");
        }

        if (!isNullOrEmpty(options.getServiceAccountKeyFile())) {
          // A key file is specified, use email-address and p12 based authentication.
          checkArgument(
              !isNullOrEmpty(options.getServiceAccountEmail()),
              "Email must be set if using service account auth and a key file is specified.");
        }
      } else if (!isNullOrEmpty(options.getClientId())) {
        checkArgument(
            !isNullOrEmpty(options.getClientSecret()),
            "clientSecret must be set if using OAuth-based Installed App authentication.");
        checkArgument(
            !isNullOrEmpty(options.getOAuthCredentialFile()),
            "credentialFile must be set if using OAuth-based Installed App authentication.");
      } else {
        checkArgument(
            options.isNullCredentialEnabled(),
            "No valid credential configuration discovered: ",
            options);
      }

      return options;
    }
  }
}
