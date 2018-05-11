package com.google.cloud.hadoop.util;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given an {@link AccessTokenProviderClassFromConfigFactory} and a Hadoop {@link Configuration},
 * generate a {@link Credential}.
 */
public final class CredentialFromAccessTokenProviderClassFactory {

  /**
   * A wrapper class that exposes a {@link GoogleCredential} interface using an {@link
   * AccessTokenProvider}.
   */
  static final class GoogleCredentialWithAccessTokenProvider extends GoogleCredential {
    private final Clock clock;
    private final AccessTokenProvider accessTokenProvider;

    private GoogleCredentialWithAccessTokenProvider(
        Clock clock, AccessTokenProvider accessTokenProvider) {
      this.clock = clock;
      this.accessTokenProvider = accessTokenProvider;
    }

    static GoogleCredential fromAccessTokenProvider(
        Clock clock, AccessTokenProvider accessTokenProvider) {
      GoogleCredentialWithAccessTokenProvider withProvider =
          new GoogleCredentialWithAccessTokenProvider(clock, accessTokenProvider);
      AccessToken accessToken =
          Preconditions.checkNotNull(
              accessTokenProvider.getAccessToken(), "Access Token cannot be null!");

      withProvider
          .setAccessToken(accessToken.getToken())
          .setExpirationTimeMilliseconds(accessToken.getExpirationTimeMilliSeconds());
      return withProvider;
    }

    @Override
    protected TokenResponse executeRefreshToken() throws IOException {
      accessTokenProvider.refresh();
      AccessToken accessToken =
          Preconditions.checkNotNull(
              accessTokenProvider.getAccessToken(), "Access Token cannot be null!");

      String token =
          Preconditions.checkNotNull(accessToken.getToken(), "Access Token cannot be null!");
      Long expirationTimeMilliSeconds = accessToken.getExpirationTimeMilliSeconds();
      return new TokenResponse()
          .setAccessToken(token)
          .setExpiresInSeconds(
              expirationTimeMilliSeconds == null
                  ? null
                  : (expirationTimeMilliSeconds - clock.currentTimeMillis()) / 1000);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(CredentialFromAccessTokenProviderClassFactory.class);

  /**
   * Generate the credential.
   *
   * <p>If the {@link AccessTokenProviderClassFromConfigFactory} generates no Class for the
   * provider, return null.
   */
  public static Credential credential(
      AccessTokenProviderClassFromConfigFactory providerClassFactory,
      Configuration config,
      Collection<String> scopes)
      throws IOException, GeneralSecurityException {
    Class<? extends AccessTokenProvider> clazz =
        providerClassFactory.getAccessTokenProviderClass(config);
    if (clazz != null) {
      LOG.debug("Using AccessTokenProvider ({})", clazz.getName());
      try {
        AccessTokenProvider accessTokenProvider = clazz.getDeclaredConstructor().newInstance();
        accessTokenProvider.setConf(config);
        return getCredentialFromAccessTokenProvider(accessTokenProvider, scopes);
      } catch (ReflectiveOperationException ex) {
        throw new IOException("Can't instantiate " + clazz.getName(), ex);
      }
    }
    return null;
  }

  /** Creates ad {@link Credential} based on information from the access token provider. */
  private static Credential getCredentialFromAccessTokenProvider(
      AccessTokenProvider accessTokenProvider, Collection<String> scopes)
      throws IOException, GeneralSecurityException {
    Preconditions.checkArgument(
        accessTokenProvider.getAccessToken() != null, "Access Token cannot be null!");
    GoogleCredential credential =
        GoogleCredentialWithAccessTokenProvider.fromAccessTokenProvider(
            Clock.SYSTEM, accessTokenProvider);
    return credential.createScoped(scopes);
  }
}
