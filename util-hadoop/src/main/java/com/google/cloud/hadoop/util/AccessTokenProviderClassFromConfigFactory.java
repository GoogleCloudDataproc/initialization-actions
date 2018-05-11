package com.google.cloud.hadoop.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory to produce the class that implements the AccessTokenProvider, from the Hadoop
 * configuration.
 */
public final class AccessTokenProviderClassFromConfigFactory {

  public static final String ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX = ".auth.access.token.provider.impl";

  private String configPrefix = "google.cloud";

  /**
   * Overrides the default 'google.cloud' configuration prefix.
   *
   * @param prefix the new prefix to override the default configuration prefix.
   * @return this object.
   */
  public AccessTokenProviderClassFromConfigFactory withOverridePrefix(String prefix) {
    this.configPrefix = prefix;
    return this;
  }

  /**
   * Get the class that implements the AccessTokenProvider, from Hadoop {@link Configuration}.
   *
   * @param config Hadoop {@link Configuration}
   * @return the class object of the class that implements the AccessTokenProvider, null if there
   *     are no such classes.
   */
  public Class<? extends AccessTokenProvider> getAccessTokenProviderClass(Configuration config) {
    return config.getClass(
        configPrefix + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX, null, AccessTokenProvider.class);
  }
}
