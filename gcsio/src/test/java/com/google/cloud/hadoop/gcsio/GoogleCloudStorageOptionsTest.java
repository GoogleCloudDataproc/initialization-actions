/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.util.RedactedString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleCloudStorageOptionsTest {

  @Test
  public void build_throwsException_whenMaxBytesRewrittenPerCallNotMbMultiple() {
    long maxBytesRewrittenPerCall = 1;
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder().setMaxBytesRewrittenPerCall(maxBytesRewrittenPerCall);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "maxBytesRewrittenPerCall must be an integral multiple of 1 MiB (1048576), but was: "
                + maxBytesRewrittenPerCall);
  }

  @Test
  public void build_throwsException_whenProxyNotSetAndPasswordNotNull() {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder()
            .setProxyPassword(RedactedString.create("proxy-password"));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
  }

  @Test
  public void build_throwsException_whenProxyNotSetAndUsernameNotNull() {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder()
            .setProxyUsername(RedactedString.create("proxy-username"));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
  }

  @Test
  public void build_throwsException_whenProxySetAndUsernameNull() {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder()
            .setProxyAddress("proxy-address")
            .setProxyPassword(RedactedString.create("proxy-password"))
            .setProxyUsername(null);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("both proxyUsername and proxyPassword should be null or not null together");
  }

  @Test
  public void build_throwsException_whenProxySetAndPasswordNull() {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder()
            .setProxyAddress("proxy-address")
            .setProxyPassword(null)
            .setProxyUsername(RedactedString.create("proxy-username"));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("both proxyUsername and proxyPassword should be null or not null together");
  }

  @Test
  public void encryptionKey_hide_whenSetEncryptionKey() {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageOptions.builder()
            .setEncryptionAlgorithm("AES256")
            .setEncryptionKey(RedactedString.create("test-key"))
            .setEncryptionKeyHash(RedactedString.create("test-key-hash"));
    GoogleCloudStorageOptions options = builder.build();

    String optionsString = options.toString();

    assertThat(optionsString).contains("encryptionKey=<redacted>");
    assertThat(optionsString).contains("encryptionKeyHash=<redacted>");
  }
}
