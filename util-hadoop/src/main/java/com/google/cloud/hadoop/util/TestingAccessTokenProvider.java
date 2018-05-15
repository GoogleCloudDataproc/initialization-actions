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
