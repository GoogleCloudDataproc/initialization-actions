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

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/** Configuration for how components should obtain Credentials. */
public class CredentialConfiguration {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private boolean serviceAccountEnabled = true;

  // The following 2 parameters are used for credentials set directly via Hadoop Configuration
  private String serviceAccountPrivateKeyId = null;
  private String serviceAccountPrivateKey = null;

  // The following 2 parameters are used for ServiceAccount P12 KeyFiles
  private String serviceAccountEmail = null;
  private String serviceAccountKeyFile = null;

  // The following parameter is used for ServiceAccount Json KeyFiles
  private String serviceAccountJsonKeyFile = null;

  // The following 3 parameters are used for client authentication
  private String clientId = null;
  private String clientSecret = null;
  private String oAuthCredentialFile = null;

  private boolean nullCredentialEnabled = false;
  private CredentialFactory credentialFactory = new CredentialFactory();
  private HttpTransportType transportType = HttpTransportType.JAVA_NET;
  private String proxyAddress = null;
  private HttpTransport transport;

  /**
   * Get the credential as configured.
   *
   * <p>The following is the order in which properties are applied to create the Credential:
   *
   * <ol>
   *   <li>If service accounts are not disabled and no service account key file or service account
   *       parameters are set, use the metadata service.
   *   <li>If service accounts are not disabled and a service-account email and keyfile, or service
   *       account parameters are provided, use service account authentication with the given
   *       parameters.
   *   <li>If service accounts are disabled and client id, client secret and OAuth credential file
   *       is provided, use the Installed App authentication flow.
   *   <li>If service accounts are disabled and null credentials are enabled for unit testing,
   *       return null
   * </ol>
   *
   * @throws IllegalStateException if none of the above conditions are met and a Credential cannot
   *     be created
   */
  public Credential getCredential(List<String> scopes)
      throws IOException, GeneralSecurityException {

    if (isServiceAccountEnabled()) {
      logger.atFine().log("Using service account credentials");

      // By default, we want to use service accounts with the meta-data service (assuming we're
      // running in GCE).
      if (shouldUseMetadataService()) {
        logger.atFine().log("Getting service account credentials from meta data service.");
        // TODO(user): Validate the returned credential has access to the given scopes.
        return credentialFactory.getCredentialFromMetadataServiceAccount();
      }

      if (!isNullOrEmpty(serviceAccountPrivateKeyId)) {
        // TODO: Test with a hadoop credentials (jceks) file that getPassword is being used.
        logger.atFine().log("Attempting to get credentials from Configuration");
        Preconditions.checkState(
            !isNullOrEmpty(serviceAccountPrivateKey),
            "privateKeyId must be set if using credentials configured directly in configuration");
        Preconditions.checkState(
            !isNullOrEmpty(serviceAccountEmail),
            "clientEmail must be set if using credentials configured directly in configuration");
        Preconditions.checkArgument(
            isNullOrEmpty(serviceAccountKeyFile),
            "A P12 key file may not be specified at the same time as credentials"
                + " via configuration.");
        Preconditions.checkArgument(
            isNullOrEmpty(serviceAccountJsonKeyFile),
            "A JSON key file may not be specified at the same time as credentials"
                + " via configuration.");
        return credentialFactory.getCredentialsFromSAParameters(
            serviceAccountPrivateKeyId,
            serviceAccountPrivateKey,
            serviceAccountEmail,
            scopes,
            getTransport());
      }

      if (!isNullOrEmpty(serviceAccountJsonKeyFile)) {
        logger.atFine().log("Using JSON keyfile %s", serviceAccountJsonKeyFile);
        Preconditions.checkArgument(
            isNullOrEmpty(serviceAccountKeyFile),
            "A P12 key file may not be specified at the same time as a JSON key file.");
        Preconditions.checkArgument(
            isNullOrEmpty(serviceAccountEmail),
            "Service account email may not be specified at the same time as a JSON key file.");
        return credentialFactory.getCredentialFromJsonKeyFile(
            serviceAccountJsonKeyFile, scopes, getTransport());
      }

      if (!isNullOrEmpty(serviceAccountKeyFile)) {
        // A key file is specified, use email-address and p12 based authentication.
        Preconditions.checkState(
            !isNullOrEmpty(serviceAccountEmail),
            "Email must be set if using service account auth and a key file is specified.");
        logger.atFine().log(
            "Using service account email %s and private key file %s",
            serviceAccountEmail, serviceAccountKeyFile);

        return credentialFactory.getCredentialFromPrivateKeyServiceAccount(
            serviceAccountEmail, serviceAccountKeyFile, scopes, getTransport());
      }

      if (shouldUseApplicationDefaultCredentials()) {
        logger.atFine().log("Getting Application Default Credentials");
        return credentialFactory.getApplicationDefaultCredentials(scopes, getTransport());
      }
    } else if (oAuthCredentialFile != null && clientId != null && clientSecret != null) {
      logger.atFine().log("Using installed app credentials in file %s", oAuthCredentialFile);

      return credentialFactory.getCredentialFromFileCredentialStoreForInstalledApp(
          clientId, clientSecret, oAuthCredentialFile, scopes, getTransport());
    } else if (nullCredentialEnabled) {
      logger.atWarning().log(
          "Allowing null credentials for unit testing. This should not be used in production");

      return null;
    }

    logger.atSevere().log("Credential configuration is not valid. Configuration: %s", this);
    throw new IllegalStateException("No valid credential configuration discovered.");
  }

  private boolean shouldUseApplicationDefaultCredentials() {
    return credentialFactory.hasApplicationDefaultCredentialsConfigured();
  }

  public boolean shouldUseMetadataService() {
    return isNullOrEmpty(serviceAccountKeyFile)
        && isNullOrEmpty(serviceAccountJsonKeyFile)
        && isNullOrEmpty(serviceAccountPrivateKey)
        && !shouldUseApplicationDefaultCredentials();
  }

  public String getOAuthCredentialFile() {
    return oAuthCredentialFile;
  }

  public void setOAuthCredentialFile(String oAuthCredentialFile) {
    this.oAuthCredentialFile = oAuthCredentialFile;
  }

  public boolean isNullCredentialEnabled() {
    return nullCredentialEnabled;
  }

  public void setNullCredentialEnabled(boolean nullCredentialEnabled) {
    this.nullCredentialEnabled = nullCredentialEnabled;
  }

  public boolean isServiceAccountEnabled() {
    return serviceAccountEnabled;
  }

  public String getServiceAccountPrivateKeyId() {
    return serviceAccountPrivateKeyId;
  }

  public void setServiceAccountPrivateKeyId(String serviceAccountPrivateKeyId) {
    this.serviceAccountPrivateKeyId = serviceAccountPrivateKeyId;
  }

  public String getServiceAccountPrivateKey() {
    return serviceAccountPrivateKey;
  }

  public void setServiceAccountPrivateKey(String serviceAccountPrivateKey) {
    this.serviceAccountPrivateKey = serviceAccountPrivateKey.replace("\\n", System.lineSeparator());
  }

  public void setEnableServiceAccounts(boolean enableServiceAccounts) {
    this.serviceAccountEnabled = enableServiceAccounts;
  }

  public String getServiceAccountEmail() {
    return serviceAccountEmail;
  }

  public void setServiceAccountEmail(String serviceAccountEmail) {
    this.serviceAccountEmail = serviceAccountEmail;
  }

  public String getServiceAccountKeyFile() {
    return serviceAccountKeyFile;
  }

  public void setServiceAccountKeyFile(String serviceAccountKeyFile) {
    this.serviceAccountKeyFile = serviceAccountKeyFile;
  }

  public String getServiceAccountJsonKeyFile() {
    return serviceAccountJsonKeyFile;
  }

  public void setServiceAccountJsonKeyFile(String serviceAccountJsonKeyFile) {
    this.serviceAccountJsonKeyFile = serviceAccountJsonKeyFile;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public HttpTransportType getTransportType() {
    return transportType;
  }

  public void setTransportType(HttpTransportType transportType) {
    this.transportType = transportType;
  }

  public String getProxyAddress() {
    return proxyAddress;
  }

  public void setProxyAddress(String proxyAddress) {
    this.proxyAddress = proxyAddress;
  }

  @VisibleForTesting
  void setCredentialFactory(CredentialFactory factory) {
    this.credentialFactory = factory;
  }

  @Override
  public String toString() {
    return "CredentialConfiguration{"
        + ("serviceAccountEnabled: " + isServiceAccountEnabled() + '\n')
        + ("serviceAccountPrivateKeyId: "
            + (isNullOrEmpty(getServiceAccountPrivateKeyId())
                ? "Not Provided"
                : "Provided, but not displayed")
            + '\n')
        + ("serviceAccountPrivateKey: "
            + (isNullOrEmpty(getServiceAccountPrivateKey())
                ? "Not Provided"
                : "Provided, but not displayed")
            + '\n')
        + ("serviceAccountEmail: " + getServiceAccountEmail() + '\n')
        + ("serviceAccountKeyfile: " + getServiceAccountKeyFile() + '\n')
        + ("serviceAccountJsonKeyFile: " + getServiceAccountJsonKeyFile() + '\n')
        + ("clientId: " + getClientId() + '\n')
        + ("clientSecret: "
            + (isNullOrEmpty(getClientSecret()) ? "Not provided" : "Provided, but not displayed")
            + '\n')
        + ("oAuthCredentialFile: " + getOAuthCredentialFile() + '\n')
        + ("isNullCredentialEnabled: " + isNullCredentialEnabled() + '\n')
        + ("transportType: " + getTransportType() + '\n')
        + ("proxyAddress: " + getProxyAddress())
        + "}";
  }

  private HttpTransport getTransport() throws IOException {
    if (transport == null) {
      transport = HttpTransportFactory.createHttpTransport(getTransportType(), getProxyAddress());
    }
    return transport;
  }

  @VisibleForTesting
  void setTransport(HttpTransport transport) {
    this.transport = transport;
  }
}
