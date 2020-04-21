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
  public abstract RedactedString getServiceAccountPrivateKeyId();

  @Nullable
  public abstract RedactedString getServiceAccountPrivateKey();

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
  public abstract RedactedString getClientId();

  @Nullable
  public abstract RedactedString getClientSecret();

  @Nullable
  public abstract String getOAuthCredentialFile();

  public abstract boolean isNullCredentialEnabled();

  public abstract HttpTransportType getTransportType();

  public abstract String getTokenServerUrl();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract RedactedString getProxyUsername();

  @Nullable
  public abstract RedactedString getProxyPassword();

  /** Builder for {@link CredentialOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setServiceAccountEnabled(boolean value);

    public abstract Builder setServiceAccountPrivateKeyId(
        RedactedString serviceAccountPrivateKeyId);

    public abstract Builder setServiceAccountPrivateKey(RedactedString serviceAccountPrivateKey);

    public abstract Builder setServiceAccountEmail(String serviceAccountEmail);

    public abstract Builder setServiceAccountKeyFile(String serviceAccountKeyFile);

    public abstract Builder setServiceAccountJsonKeyFile(String serviceAccountJsonKeyFile);

    public abstract Builder setClientId(RedactedString clientId);

    public abstract Builder setClientSecret(RedactedString clientSecret);

    public abstract Builder setOAuthCredentialFile(String oAuthCredentialFile);

    public abstract Builder setNullCredentialEnabled(boolean nullCredentialEnabled);

    public abstract Builder setTransportType(HttpTransportType transportType);

    public abstract Builder setTokenServerUrl(String tokenServerUrl);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(RedactedString proxyUsername);

    public abstract Builder setProxyPassword(RedactedString proxyPassword);

    abstract CredentialOptions autoBuild();

    public CredentialOptions build() {
      CredentialOptions options = autoBuild();

      if (options.isServiceAccountEnabled()) {
        if (options.getServiceAccountPrivateKeyId() != null) {
          checkArgument(
              options.getServiceAccountPrivateKey() != null,
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
      } else if (options.getClientId() != null) {
        checkArgument(
            options.getClientSecret() != null,
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
