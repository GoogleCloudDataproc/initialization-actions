package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Clock;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.api.services.iamcredentials.v1.IAMCredentials.Projects.ServiceAccounts.GenerateAccessToken;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;

/** A {@code Credential} to generate or refresh IAM access token. */
public class GoogleCredentialWithIamAccessToken extends GoogleCredential {

  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private static final String DEFAULT_ACCESS_TOKEN_LIFETIME = "3600s";
  private static final String DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX = "projects/-/serviceAccounts/";

  private final HttpRequestInitializer initializer;
  private final HttpTransport transport;
  private final String serviceAccountResource;
  private final ImmutableList<String> scopes;
  private final Clock clock;

  public GoogleCredentialWithIamAccessToken(
      HttpTransport transport,
      HttpRequestInitializer initializer,
      String serviceAccountName,
      ImmutableList<String> scopes)
      throws IOException {
    this(transport, initializer, serviceAccountName, scopes, Clock.SYSTEM);
  }

  @VisibleForTesting
  public GoogleCredentialWithIamAccessToken(
      HttpTransport transport,
      HttpRequestInitializer initializer,
      String serviceAccountName,
      ImmutableList<String> scopes,
      Clock clock)
      throws IOException {
    this.serviceAccountResource = DEFAULT_SERVICE_ACCOUNT_NAME_PREFIX + serviceAccountName;
    this.initializer = initializer;
    this.transport = transport;
    this.scopes = scopes;
    this.clock = clock;
    initialize();
  }

  private void initialize() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    if (!isNullOrEmpty(accessTokenResponse.getExpireTime())) {
      Instant expireTimeInstant = Instant.parse(accessTokenResponse.getExpireTime());
      setExpirationTimeMilliseconds(expireTimeInstant.toEpochMilli());
    }
    setAccessToken(accessTokenResponse.getAccessToken());
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    GenerateAccessTokenResponse accessTokenResponse = generateAccessToken();
    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken(accessTokenResponse.getAccessToken());

    if (isNullOrEmpty(accessTokenResponse.getExpireTime())) {
      return tokenResponse;
    }

    Instant expirationTimeInInstant = Instant.parse(accessTokenResponse.getExpireTime());
    long expirationTimeMilliSeconds = expirationTimeInInstant.getEpochSecond();
    return tokenResponse.setExpiresInSeconds(
        expirationTimeMilliSeconds - clock.currentTimeMillis() / 1000);
  }

  private GenerateAccessTokenResponse generateAccessToken() throws IOException {
    GenerateAccessTokenRequest requestContent =
        new GenerateAccessTokenRequest()
            .setScope(scopes)
            .setLifetime(DEFAULT_ACCESS_TOKEN_LIFETIME);
    GenerateAccessToken request =
        new IAMCredentials(transport, JSON_FACTORY, initializer)
            .projects()
            .serviceAccounts()
            .generateAccessToken(serviceAccountResource, requestContent);
    GenerateAccessTokenResponse response = request.execute();
    checkNotNull(response.getAccessToken(), "Access Token cannot be null!");
    return response;
  }
}
