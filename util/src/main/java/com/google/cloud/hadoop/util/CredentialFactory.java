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

import static com.google.common.base.Preconditions.checkArgument;
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
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
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

  private static final String TOKEN_SERVER_URL_ENV_VAR = "GOOGLE_OAUTH_TOKEN_SERVER_URL";
  private static final String TOKEN_SERVER_URL_DEFAULT = "https://oauth2.googleapis.com/token";
  private static final String TOKEN_SERVER_URL =
      MoreObjects.firstNonNull(System.getenv(TOKEN_SERVER_URL_ENV_VAR), TOKEN_SERVER_URL_DEFAULT);

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

    /** Create a new GoogleCredentialWithRetry from a GoogleCredential. */
    public static GoogleCredentialWithRetry fromGoogleCredential(GoogleCredential credential) {
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
      GoogleCredentialWithRetry withRetry = new GoogleCredentialWithRetry(builder);
      // Setting a refresh token requires validation even if it is null.
      if (credential.getRefreshToken() != null) {
        withRetry.setRefreshToken(credential.getRefreshToken());
      }
      return withRetry;
    }

    public GoogleCredentialWithRetry(Builder builder) {
      super(builder.setTokenServerEncodedUrl(TOKEN_SERVER_URL));
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
      payload.put("scope", Joiner.on(' ').join(getServiceAccountScopes()));

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
      } catch (GeneralSecurityException exception) {
        throw (IOException) new IOException().initCause(exception);
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

  /**
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local GCE VM.
   * See: <a href="https://developers.google.com/compute/docs/authentication">Authenticating from
   * Google Compute Engine</a>.
   */
  public Credential getCredentialFromMetadataServiceAccount()
      throws IOException, GeneralSecurityException {
    logger.atFine().log("getCredentialFromMetadataServiceAccount()");
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
   * @param serviceAccountEmail Email address of the service account associated with the keyfile.
   * @param privateKeyFile Full local path to private keyfile.
   * @param scopes List of well-formed desired scopes to use with the credential.
   * @param transport The HttpTransport used for authorization
   */
  public Credential getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail,
      String privateKeyFile,
      List<String> scopes,
      HttpTransport transport)
      throws IOException, GeneralSecurityException {
    logger.atFine().log(
        "getCredentialFromPrivateKeyServiceAccount(%s, %s, %s)",
        serviceAccountEmail, privateKeyFile, scopes);

    return new GoogleCredentialWithRetry(
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(JSON_FACTORY)
            .setServiceAccountId(serviceAccountEmail)
            .setServiceAccountScopes(scopes)
            .setServiceAccountPrivateKeyFromP12File(new File(privateKeyFile))
            .setRequestInitializer(new CredentialHttpRetryInitializer()));
  }

  /**
   * Get credentials listed in a JSON file.
   *
   * @param serviceAccountJsonKeyFile A file path pointing to a JSON file containing credentials.
   * @param scopes The OAuth scopes that the credential should be valid for.
   * @param transport The HttpTransport used for authorization
   */
  public Credential getCredentialFromJsonKeyFile(
      String serviceAccountJsonKeyFile, List<String> scopes, HttpTransport transport)
      throws IOException, GeneralSecurityException {
    logger.atFine().log("getCredentialFromJsonKeyFile(%s, %s)", serviceAccountJsonKeyFile, scopes);

    try (FileInputStream fis = new FileInputStream(serviceAccountJsonKeyFile)) {
      return GoogleCredentialWithRetry.fromGoogleCredential(
          GoogleCredential.fromStream(fis, transport, JSON_FACTORY).createScoped(scopes));
    }
  }

  public Credential getCredentialsFromSAParameters(
      String privateKeyId,
      String privateKeyPem,
      String serviceAccountEmail,
      List<String> scopes,
      HttpTransport transport)
      throws IOException {
    logger.atFine().log(
        "getServiceAccountCredentialFromHadoopConfiguration(%s)", serviceAccountEmail);
    if (serviceAccountEmail == null || privateKeyPem == null || privateKeyId == null) {
      throw new IOException(
          "Error reading service account credential from stream, "
              + "expecting, 'client_email', 'private_key' and 'private_key_id'.");
    }
    PrivateKey privateKey = privateKeyFromPkcs8(privateKeyPem);
    GoogleCredential.Builder builder =
        new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(JSON_FACTORY)
            .setServiceAccountId(serviceAccountEmail)
            .setServiceAccountScopes(scopes)
            .setServiceAccountPrivateKey(privateKey)
            .setServiceAccountPrivateKeyId(privateKeyId);
    return new GoogleCredentialWithRetry(builder);
  }

  /**
   * Initialized OAuth2 credential for the "installed application" flow; where the credential
   * typically represents an actual end user (instead of a service account), and is stored as a
   * refresh token in a local FileCredentialStore.
   *
   * @param clientId OAuth2 client ID identifying the 'installed app'
   * @param clientSecret OAuth2 client secret
   * @param filePath full path to a ".json" file for storing the credential
   * @param scopes list of well-formed scopes desired in the credential
   * @param transport The HttpTransport used for authorization
   * @return credential with desired scopes, possibly obtained from loading {@code filePath}.
   * @throws IOException on IO error
   */
  public Credential getCredentialFromFileCredentialStoreForInstalledApp(
      String clientId,
      String clientSecret,
      String filePath,
      List<String> scopes,
      HttpTransport transport)
      throws IOException, GeneralSecurityException {
    logger.atFine().log(
        "getCredentialFromFileCredentialStoreForInstalledApp(%s, %s, %s, %s)",
        clientId, clientSecret, filePath, scopes);
    checkArgument(!isNullOrEmpty(clientId), "clientId must not be null or empty");
    checkArgument(!isNullOrEmpty(clientSecret), "clientSecret must not be null or empty");
    checkArgument(!isNullOrEmpty(filePath), "filePath must not be null or empty");
    checkNotNull(scopes, "scopes must not be null");

    // Initialize client secrets.
    GoogleClientSecrets.Details details =
        new GoogleClientSecrets.Details().setClientId(clientId).setClientSecret(clientSecret);
    GoogleClientSecrets clientSecrets = new GoogleClientSecrets().setInstalled(details);

    // Set up file credential store.
    FileCredentialStore credentialStore = new FileCredentialStore(new File(filePath), JSON_FACTORY);

    // Set up authorization code flow.
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(transport, JSON_FACTORY, clientSecrets, scopes)
            .setCredentialStore(credentialStore)
            .setRequestInitializer(new CredentialHttpRetryInitializer())
            .setTokenServerUrl(new GenericUrl(TOKEN_SERVER_URL))
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
  boolean hasApplicationDefaultCredentialsConfigured() {
    return System.getenv(CREDENTIAL_ENV_VAR) != null;
  }

  /**
   * Get Google Application Default Credentials as described in <a
   * href="https://developers.google.com/identity/protocols/application-default-credentials#callingjava"
   * >Google Application Default Credentials</a>
   *
   * @param scopes The OAuth scopes that the credential should be valid for.
   */
  public Credential getApplicationDefaultCredentials(List<String> scopes, HttpTransport transport)
      throws IOException, GeneralSecurityException {
    logger.atFine().log("getApplicationDefaultCredential(%s)", scopes);
    return GoogleCredentialWithRetry.fromGoogleCredential(
        GoogleCredential.getApplicationDefault(transport, JSON_FACTORY).createScoped(scopes));
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
      PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
      return privateKey;
    } catch (NoSuchAlgorithmException | InvalidKeySpecException exception) {
      throw new IOException("Unexpected expcetion reading PKCS data", exception);
    }
  }
}
