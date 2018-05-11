package com.google.cloud.hadoop.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;

/** A provider to provide access token, and upon access token expiration, the utility to refresh. */
public interface AccessTokenProvider extends Configurable {

  /** An access token and its expiration time. */
  class AccessToken {

    private final String token;
    private final Long expirationTimeMilliSeconds;

    public AccessToken(String token, Long expirationTimeMillis) {
      this.token = token;
      this.expirationTimeMilliSeconds = expirationTimeMillis;
    }

    /** @return the Access Token string. */
    public String getToken() {
      return token;
    }

    /** @return the Time when the token will expire, expressed in milliseconds. */
    public Long getExpirationTimeMilliSeconds() {
      return expirationTimeMilliSeconds;
    }
  }

  /** @return an access token. */
  AccessToken getAccessToken();

  /**
   * Force this provider to refresh its access token.
   *
   * @throws IOException when refresh fails.
   */
  void refresh() throws IOException;
}
