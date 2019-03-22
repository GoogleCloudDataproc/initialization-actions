/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

public class GoogleHadoopFileSystemDelegationTokensTest {

  /** Verifies that a configured delegation token binding is correctly loaded and employed */
  @Test
  public void testDelegationTokenBinding() {
    final URI initUri = (new Path("gs://" + "test/")).toUri();
    final Text expectedKind = TestTokenIdentifierImpl.KIND;

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    try {
      fs.initialize(initUri, loadConfig());

      // Request a delegation token
      Token<?> dt = fs.getDelegationToken(null);
      assertNotNull("Expected a delegation token", dt);
      assertEquals("Unexpected delegation token service", "gs://test", dt.getService().toString());
      assertEquals("Unexpected delegation token kind", expectedKind, dt.getKind());

      // Validate the associated identifier
      TokenIdentifier decoded = dt.decodeIdentifier();
      assertNotNull("Failed to decode token identifier", decoded);
      assertTrue(
          "Unexpected delegation token identifier type",
          (decoded instanceof TestTokenIdentifierImpl));

      DelegationTokenIdentifier identifier = (DelegationTokenIdentifier) decoded;
      assertEquals(
          "Unexpected delegation token identifier kind", expectedKind, identifier.getKind());
    } catch (IOException e) {
      fail(e.getMessage());
    }
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
