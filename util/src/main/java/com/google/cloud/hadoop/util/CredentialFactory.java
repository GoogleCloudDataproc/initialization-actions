/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.FileCredentialStore;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.PemReader;
import com.google.api.client.util.PemReader.Section;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;

/** Miscellaneous helper methods for getting a {@code Credential} from various sources. */
public class CredentialFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  static final String CREDENTIAL_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";

  /**
   * Simple HttpRequestInitializer that retries requests that result in 5XX response codes and IO
   * Exceptions with an exponential backoff.
   */
  public static class CredentialHttpRetryInitializer implements HttpRequestInitializer {

    @Override
    public void initialize(HttpRequest httpRequest) throws IOException {
      httpRequest.setIOExceptionHandler(
          new HttpBackOffIOExceptionHandler(new ExponentialBackOff()));
      httpRequest.setUnsuccessfulResponseHandler(
          new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));
    }
  }

  /**
   * A subclass of {@link GoogleCredential} that properly wires specified {@link
   * HttpRequestInitializer} through the @{link Credential#executeRefreshToken} override.
   *
   * <p>We will not retry 403 "invalid_request" rate limiting errors. See the following for more on
   * rate limiting in OAuth: https://code.google.com/p/google-api-java-client/issues/detail?id=879
   */
  public static class GoogleCredentialWithRetry extends GoogleCredential {

    private static final int DEFAULT_TOKEN_EXPIRATION_SECONDS = 3600;

    private static final Joiner WHITESPACE_JOINER = Joiner.on(' ');

    /** Create a new GoogleCredentialWithRetry from a GoogleCredential. */
    static GoogleCredentialWithRetry fromGoogleCredential(
        GoogleCredential credential, String tokenServerUrl) {
      GoogleCredential.Builder builder =
          new GoogleCredential.Builder()
              .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
              .setServiceAccountPrivateKeyId(credential.getServiceAccountPrivateKeyId())
              .setServiceAccountId(credential.getServiceAccountId())
              .setServiceAccountUser(credential.getServiceAccountUser())
              .setServiceAccountScopes(credential.getServiceAccountScopes())
              .setTokenServerEncodedUrl(credential.getTokenServerEncodedUrl())
              .setTransport(credential.getTransport())
              .setClientAuthentication(credential.getClientAuthentication())
              .setJsonFactory(credential.getJsonFactory())
              .setClock(credential.getClock())
              .setRequestInitializer(new CredentialHttpRetryInitializer());
      GoogleCredentialWithRetry withRetry = new GoogleCredentialWithRetry(builder, tokenServerUrl);
      // Setting a refresh token requires validation even if it is null.
      if (credential.getRefreshToken() != null) {
        withRetry.setRefreshToken(credential.getRefreshToken());
      }
      return withRetry;
    }

    GoogleCredentialWithRetry(Builder builder, String tokenServerUrl) {
      super(builder.setTokenServerEncodedUrl(tokenServerUrl));
    }

    @Override
    protected TokenResponse executeRefreshToken() throws IOException {
      if (getServiceAccountPrivateKey() == null) {
        return super.executeRefreshToken();
      }
      // service accounts: no refresh token; instead use private key to request new access token
      JsonWebSignature.Header header =
          new JsonWebSignature.Header()
              .setAlgorithm("RS256")
              .setType("JWT")
              .setKeyId(getServiceAccountPrivateKeyId());

      long currentTime = getClock().currentTimeMillis();
      JsonWebToken.Payload payload =
          new JsonWebToken.Payload()
              .setIssuer(getServiceAccountId())
              .setAudience(getTokenServerEncodedUrl())
              .setIssuedAtTimeSeconds(currentTime / 1000)
              .setExpirationTimeSeconds(currentTime / 1000 + DEFAULT_TOKEN_EXPIRATION_SECONDS)
              .setSubject(getServiceAccountUser());
      payload.put("scope", WHITESPACE_JOINER.join(getServiceAccountScopes()));

      try {
        String assertion =
            JsonWebSignature.signUsingRsaSha256(
                getServiceAccountPrivateKey(), getJsonFactory(), header, payload);
        TokenRequest request =
            new TokenRequest(
                    getTransport(),
                    getJsonFactory(),
                    new GenericUrl(getTokenServerEncodedUrl()),
                    "urn:ietf:params:oauth:grant-type:jwt-bearer")
                .setRequestInitializer(getRequestInitializer());
        request.put("assertion", assertion);
        return request.execute();
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to refresh token", e);
      }
    }
  }

  /** A subclass of ComputeCredential that properly sets request initializers. */
  public static class ComputeCredentialWithRetry extends ComputeCredential {

    public ComputeCredentialWithRetry(Builder builder) {
      super(builder);
    }

    @Override
    protected TokenResponse executeRefreshToken() throws IOException {
      HttpRequest request =
          getTransport()
              .createRequestFactory(getRequestInitializer())
              .buildGetRequest(new GenericUrl(getTokenServerEncodedUrl()))
              .setParser(new JsonObjectParser(getJsonFactory()));
      request.getHeaders().set("Metadata-Flavor", "Google");
      return request.execute().parseAs(TokenResponse.class);
    }
  }

  // List of GCS scopes to specify when obtaining a credential.
  public static final ImmutableList<String> GCS_SCOPES =
      ImmutableList.of(StorageScopes.DEVSTORAGE_FULL_CONTROL);

  // JSON factory used for formatting credential-handling payloads.
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  // HTTP transport used for created credentials to perform token-refresh handshakes with remote
  // credential servers. Initialized lazily to move the possibility of throwing
  // GeneralSecurityException to the time a caller actually tries to get a credential.
  // Should only be used for Metadata Auth.
  private static HttpTransport staticHttpTransport = null;

  /**
   * Returns shared staticHttpTransport instance; initializes staticHttpTransport if it hasn't
   * already been initialized.
   */
  private static synchronized HttpTransport getStaticHttpTransport()
      throws IOException, GeneralSecurityException {
    if (staticHttpTransport == null) {
      staticHttpTransport = HttpTransportFactory.createHttpTransport(HttpTransportType.JAVA_NET);
    }
    return staticHttpTransport;
  }

  @VisibleForTesting
  static synchronized void setStaticHttpTransport(HttpTransport transport) {
    staticHttpTransport = transport;
  }

  private final CredentialOptions options;

  private HttpTransport transport;

  public CredentialFactory(CredentialOptions options) {
    this.options = options;
  }

  /**
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local GCE VM.
   * See: <a href="https://developers.google.com/compute/docs/authentication">Authenticating from
   * Google Compute Engine</a>.
   */
  public static Credential getCredentialFromMetadataServiceAccount()
      throws IOException, GeneralSecurityException {
    logger.atFine().log("Getting service account credentials from meta data service.");
    Credential cred =
        new ComputeCredentialWithRetry(
            new ComputeCredential.Builder(getStaticHttpTransport(), JSON_FACTORY)
                .setRequestInitializer(new CredentialHttpRetryInitializer()));
    try {
      cred.refreshToken();
    } catch (IOException e) {
      throw new IOException(
          "Error getting access token from metadata server at: " + cred.getTokenServerEncodedUrl(),
          e);
    }
    return cred;
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in <a
   * href="https://code.google.com/p/google-api-java-client/wiki/OAuth2#Service_Accounts" > OAuth2
   * Service Accounts</a>.
   *
   * @param scopes List of well-formed desired scopes to use with the credential.
   * @param transport The HttpTransport used for authorization
   */
  public Credential getCredentialFromPrivateKeyServiceAccount(
      List<String> scopes, HttpTransport transport) throws IOException, GeneralSecurityException {
    logger.atFine().log(
        "getCredentialFromPrivateKeyServiceAccount(%s, %s) for '%s' from '%s'",
        scopes, transport, options.getServiceAccountEmail(), options.getServiceAccountKeyFile());

    return new GoogleCredentialWithRetry(
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(JSON_FACTORY)
            .setServiceAccountId(options.getServiceAccountEmail())
            .setServiceAccountScopes(scopes)
            .setServiceAccountPrivateKeyFromP12File(new File(options.getServiceAccountKeyFile()))
            .setRequestInitializer(new CredentialHttpRetryInitializer()),
        options.getTokenServerUrl());
  }

  /**
   * Get credentials listed in a JSON file.
   *
   * @param scopes The OAuth scopes that the credential should be valid for.
   * @param transport The HttpTransport used for authorization
   */
  private Credential getCredentialFromJsonKeyFile(List<String> scopes, HttpTransport transport)
      throws IOException {
    logger.atFine().log(
        "getCredentialFromJsonKeyFile(%s, %s) from '%s'",
        scopes, transport, options.getServiceAccountJsonKeyFile());

    try (FileInputStream fis = new FileInputStream(options.getServiceAccountJsonKeyFile())) {
      return GoogleCredentialWithRetry.fromGoogleCredential(
          GoogleCredential.fromStream(fis, transport, JSON_FACTORY).createScoped(scopes),
          options.getTokenServerUrl());
    }
  }

  private Credential getCredentialsFromSAParameters(List<String> scopes, HttpTransport transport)
      throws IOException {
    logger.atFine().log("getCredentialsFromSAParameters(%s, %s)", scopes, transport);
    GoogleCredential.Builder builder =
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(JSON_FACTORY)
            .setServiceAccountId(options.getServiceAccountEmail())
            .setServiceAccountScopes(scopes)
            .setServiceAccountPrivateKey(
                privateKeyFromPkcs8(options.getServiceAccountPrivateKey().value()))
            .setServiceAccountPrivateKeyId(options.getServiceAccountPrivateKeyId().value());
    return new GoogleCredentialWithRetry(builder, options.getTokenServerUrl());
  }

  /**
   * Initialized OAuth2 credential for the "installed application" flow; where the credential
   * typically represents an actual end user (instead of a service account), and is stored as a
   * refresh token in a local FileCredentialStore.
   *
   * @param scopes list of well-formed scopes desired in the credential
   * @param transport The HttpTransport used for authorization
   * @return credential with desired scopes, possibly obtained from loading {@code filePath}.
   * @throws IOException on IO error
   */
  private Credential getCredentialFromFileCredentialStoreForInstalledApp(
      List<String> scopes, HttpTransport transport) throws IOException {
    logger.atFine().log(
        "getCredentialFromFileCredentialStoreForInstalledApp(%s, %s) from '%s'",
        scopes, transport, options.getOAuthCredentialFile());

    // Initialize client secrets.
    GoogleClientSecrets.Details details =
        new GoogleClientSecrets.Details()
            .setClientId(options.getClientId().value())
            .setClientSecret(options.getClientSecret().value());
    GoogleClientSecrets clientSecrets = new GoogleClientSecrets().setInstalled(details);

    // Set up file credential store.
    FileCredentialStore credentialStore =
        new FileCredentialStore(new File(options.getOAuthCredentialFile()), JSON_FACTORY);

    // Set up authorization code flow.
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(transport, JSON_FACTORY, clientSecrets, scopes)
            .setCredentialStore(credentialStore)
            .setRequestInitializer(new CredentialHttpRetryInitializer())
            .setTokenServerUrl(new GenericUrl(options.getTokenServerUrl()))
            .build();

    // Authorize access.
    return new AuthorizationCodeInstalledApp(flow, new GooglePromptReceiver()).authorize("user");
  }

  /**
   * Determines whether Application Default Credentials have been configured as an evironment
   * variable.
   *
   * <p>In this class for testability.
   */
  private static boolean isApplicationDefaultCredentialsConfigured() {
    return System.getenv(CREDENTIAL_ENV_VAR) != null;
  }

  /**
   * Get Google Application Default Credentials as described in <a
   * href="https://developers.google.com/identity/protocols/application-default-credentials#callingjava"
   * >Google Application Default Credentials</a>
   *
   * @param scopes The OAuth scopes that the credential should be valid for.
   */
  private Credential getApplicationDefaultCredentials(List<String> scopes, HttpTransport transport)
      throws IOException {
    logger.atFine().log("getApplicationDefaultCredential(%s, %s)", scopes, transport);
    return GoogleCredentialWithRetry.fromGoogleCredential(
        GoogleCredential.getApplicationDefault(transport, JSON_FACTORY).createScoped(scopes),
        options.getTokenServerUrl());
  }

  // TODO: Copied (mostly) over from Google Credential since it has private scope
  private static PrivateKey privateKeyFromPkcs8(String privateKeyPem) throws IOException {
    Reader reader = new StringReader(privateKeyPem);
    Section section = PemReader.readFirstSectionAndClose(reader, "PRIVATE KEY");
    if (section == null) {
      throw new IOException("Invalid PKCS8 data.");
    }
    byte[] bytes = section.getBase64DecodedBytes();
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
    try {
      KeyFactory keyFactory = SecurityUtils.getRsaKeyFactory();
      return keyFactory.generatePrivate(keySpec);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException exception) {
      throw new IOException("Unexpected exception reading PKCS data", exception);
    }
  }

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
    checkNotNull(scopes, "scopes must not be null");

    if (options.isServiceAccountEnabled()) {
      logger.atFine().log("Using service account credentials");

      // By default, we want to use service accounts with the meta-data service
      // (assuming we're running in GCE).
      if (useMetadataService()) {
        // TODO(user): Validate the returned credential has access to the given scopes.
        return getCredentialFromMetadataServiceAccount();
      }

      if (options.getServiceAccountPrivateKeyId() != null) {
        return getCredentialsFromSAParameters(scopes, getTransport());
      }

      if (!isNullOrEmpty(options.getServiceAccountJsonKeyFile())) {
        return getCredentialFromJsonKeyFile(scopes, getTransport());
      }

      if (!isNullOrEmpty(options.getServiceAccountKeyFile())) {
        return getCredentialFromPrivateKeyServiceAccount(scopes, getTransport());
      }

      if (isApplicationDefaultCredentialsConfigured()) {
        return getApplicationDefaultCredentials(scopes, getTransport());
      }
    } else if (options.getClientId() != null) {
      return getCredentialFromFileCredentialStoreForInstalledApp(scopes, getTransport());
    } else if (options.isNullCredentialEnabled()) {
      logger.atWarning().log(
          "Allowing null credentials for unit testing. This should not be used in production");
      return null;
    }

    throw new IllegalStateException("No valid credential configuration discovered: " + this);
  }

  private boolean useMetadataService() {
    return isNullOrEmpty(options.getServiceAccountKeyFile())
        && isNullOrEmpty(options.getServiceAccountJsonKeyFile())
        && options.getServiceAccountPrivateKey() == null
        && !isApplicationDefaultCredentialsConfigured();
  }

  private HttpTransport getTransport() throws IOException {
    if (transport == null) {
      transport =
          HttpTransportFactory.createHttpTransport(
              options.getTransportType(),
              options.getProxyAddress(),
              options.getProxyUsername(),
              options.getProxyPassword());
    }
    return transport;
  }

  @VisibleForTesting
  void setTransport(HttpTransport transport) {
    this.transport = transport;
  }
}
