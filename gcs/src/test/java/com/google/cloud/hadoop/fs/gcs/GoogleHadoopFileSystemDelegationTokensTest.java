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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.hadoop.fs.gcs.auth.TestDelegationTokenBindingImpl;
import com.google.cloud.hadoop.fs.gcs.auth.TestTokenIdentifierImpl;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for GoogleHadoopFileSystemDelegationTokens class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemDelegationTokensTest {

  /** Verifies that a configured delegation token binding is correctly loaded and employed */
  @Test
  public void testDelegationTokenBinding() throws IOException {
    URI initUri = new Path("gs://test/").toUri();
    Text expectedKind = TestTokenIdentifierImpl.KIND;

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(initUri, loadConfig());

    // Request a delegation token
    Token<?> dt = fs.getDelegationToken(null);
    assertWithMessage("Expected a delegation token").that(dt).isNotNull();
    assertWithMessage("Unexpected delegation token service")
        .that(dt.getService().toString())
        .isEqualTo("gs://test");
    assertWithMessage("Unexpected delegation token kind")
        .that(dt.getKind())
        .isEqualTo(expectedKind);

    // Validate the associated identifier
    TokenIdentifier decoded = dt.decodeIdentifier();
    assertWithMessage("Failed to decode token identifier").that(decoded).isNotNull();
    assertWithMessage("Unexpected delegation token identifier type")
        .that(decoded)
        .isInstanceOf(TestTokenIdentifierImpl.class);

    DelegationTokenIdentifier identifier = (DelegationTokenIdentifier) decoded;
    assertWithMessage("Unexpected delegation token identifier kind")
        .that(identifier.getKind())
        .isEqualTo(expectedKind);
  }

  private Configuration loadConfig() {
    Configuration config = new Configuration();

    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "test_project");
    config.setInt(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getKey(), 512);
    config.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), 1024);

    // Token binding config
    config.set(
        DELEGATION_TOKEN_BINDING_CLASS.getKey(), TestDelegationTokenBindingImpl.class.getName());
    config.set(
        TestDelegationTokenBindingImpl.TestAccessTokenProviderImpl.TOKEN_CONFIG_PROPERTY_NAME,
        "qWDAWFA3WWFAWFAWFAW3FAWF3AWF3WFAF33GR5G5"); // Bogus auth token

    return config;
  }
}
