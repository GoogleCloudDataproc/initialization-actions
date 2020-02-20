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

  public abstract Builder toBuilder();

  @Override
  public String toString() {
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

    public abstract CredentialOptions build();
  }
}
