package com.google.cloud.hadoop.testing;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

/** A mock implementation of the {@link AccessTokenProvider} interface to be used in tests. */
public final class TestingAccessTokenProvider implements AccessTokenProvider {

  public static final String FAKE_ACCESS_TOKEN = "invalid-access-token";
  public static final Long EXPIRATION_TIME_MILLISECONDS = 2000L;

  private Configuration config;

  @Override
  public AccessToken getAccessToken() {
    return new AccessToken(FAKE_ACCESS_TOKEN, EXPIRATION_TIME_MILLISECONDS);
  }

  @Override
  public void refresh() {}

  @Override
  public void setConf(Configuration config) {
    this.config = config;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }
}
