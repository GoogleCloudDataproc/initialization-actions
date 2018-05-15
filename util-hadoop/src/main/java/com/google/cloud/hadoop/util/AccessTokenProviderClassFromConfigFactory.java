/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
