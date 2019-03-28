/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs.auth;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/** A test delegation token binding implementation */
public class TestDelegationTokenBindingImpl extends AbstractDelegationTokenBinding {

  public TestDelegationTokenBindingImpl() {
    super(TestTokenIdentifierImpl.KIND);
  }

  @Override
  public AccessTokenProvider deployUnbonded() throws IOException {
    return new TestAccessTokenProviderImpl();
  }

  @Override
  public AccessTokenProvider bindToTokenIdentifier(DelegationTokenIdentifier retrievedIdentifier)
      throws IOException {
    return deployUnbonded();
  }

  @Override
  public DelegationTokenIdentifier createTokenIdentifier(Text renewer) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    return new TestTokenIdentifierImpl(owner, renewer, realUser, getService());
  }

  @Override
  public DelegationTokenIdentifier createTokenIdentifier() throws IOException {
    return createEmptyIdentifier();
  }

  @Override
  public DelegationTokenIdentifier createEmptyIdentifier() {
    return new TestTokenIdentifierImpl();
  }

  public static class TestAccessTokenProviderImpl implements AccessTokenProvider {

    public static final String TOKEN_CONFIG_PROPERTY_NAME = "test.token.value";

    private Configuration config = null;

    @Override
    public AccessToken getAccessToken() {
      return new AccessToken(
          config.get(TOKEN_CONFIG_PROPERTY_NAME), System.currentTimeMillis() + 60000);
    }

    @Override
    public void refresh() throws IOException {
      //
    }

    @Override
    public void setConf(Configuration configuration) {
      this.config = configuration;
    }

    @Override
    public Configuration getConf() {
      return config;
    }
  }
}
