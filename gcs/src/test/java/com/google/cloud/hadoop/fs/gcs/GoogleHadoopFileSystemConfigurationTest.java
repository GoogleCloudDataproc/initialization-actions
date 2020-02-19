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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.OutputStreamType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.GenerationReadConsistency;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemConfigurationTest {

  @SuppressWarnings("DoubleBraceInitialization")
  private static final Map<String, Object> expectedDefaultConfiguration =
      new HashMap<String, Object>() {
        {
          put("fs.gs.project.id", null);
          put("fs.gs.working.dir", "/");
          put("fs.gs.implicit.dir.repair.enable", true);
          put("fs.gs.copy.with.rewrite.enable", true);
          put("fs.gs.rewrite.max.bytes.per.call", 536870912);
          put("fs.gs.config.override.file", null);
          put("fs.gs.reported.permissions", "700");
          put("fs.gs.delegation.token.binding", null);
          put("fs.gs.bucket.delete.enable", false);
          put("fs.gs.checksum.type", GcsFileChecksumType.NONE);
          put("fs.gs.status.parallel.enable", false);
          put("fs.gs.lazy.init.enable", false);
          put("fs.gs.path.encoding", "uri-path");
          put("fs.gs.block.size", 67108864);
          put("fs.gs.implicit.dir.infer.enable", true);
          put("fs.gs.glob.flatlist.enable", true);
          put("fs.gs.glob.concurrent.enable", true);
          put("fs.gs.max.requests.per.batch", 15);
          put("fs.gs.batch.threads", 15);
          put("fs.gs.copy.max.requests.per.batch", 15);
          put("fs.gs.copy.batch.threads", 15);
          put("fs.gs.list.max.items.per.call", 1024);
          put("fs.gs.max.wait.for.empty.object.creation.ms", 3000);
          put("fs.gs.marker.file.pattern", null);
          put("fs.gs.auth.access.token.provider.impl", null);
          put("fs.gs.auth.service.account.enable", true);
          put("fs.gs.auth.service.account.email", null);
          put("fs.gs.auth.service.account.private.key.id", null);
          put("fs.gs.auth.service.account.private.key", null);
          put("fs.gs.auth.service.account.json.keyfile", null);
          put("fs.gs.auth.service.account.keyfile", null);
          put("fs.gs.auth.client.id", null);
          put("fs.gs.auth.client.secret", null);
          put("fs.gs.auth.client.file", null);
          put("fs.gs.inputstream.buffer.size", 0);
          put("fs.gs.inputstream.fast.fail.on.not.found.enable", true);
          put("fs.gs.inputstream.support.gzip.encoding.enable", false);
          put("fs.gs.generation.read.consistency", GenerationReadConsistency.LATEST);
          put("fs.gs.outputstream.buffer.size", 8388608);
          put("fs.gs.outputstream.pipe.buffer.size", 1048576);
          put("fs.gs.outputstream.upload.chunk.size", 67108864);
          put("fs.gs.outputstream.direct.upload.enable", false);
          put("fs.gs.outputstream.type", OutputStreamType.BASIC);
          put("fs.gs.http.transport.type", HttpTransportType.JAVA_NET);
          put("fs.gs.application.name.suffix", "");
          put("fs.gs.proxy.address", null);
          put("fs.gs.proxy.username", null);
          put("fs.gs.proxy.password", null);
          put("fs.gs.http.max.retry", 10);
          put("fs.gs.http.connect-timeout", 20000);
          put("fs.gs.http.read-timeout", 20000);
          put("fs.gs.inputstream.fadvise", Fadvise.AUTO);
          put("fs.gs.inputstream.inplace.seek.limit", 8388608);
          put("fs.gs.inputstream.min.range.request.size", 524288);
          put("fs.gs.performance.cache.enable", false);
          put("fs.gs.performance.cache.max.entry.age.ms", 5000);
          put("fs.gs.performance.cache.list.caching.enable", false);
          put("fs.gs.requester.pays.mode", RequesterPaysMode.DISABLED);
          put("fs.gs.requester.pays.project.id", null);
          put("fs.gs.requester.pays.buckets", ImmutableList.of());
          put("fs.gs.cooperative.locking.enable", false);
          put("fs.gs.cooperative.locking.expiration.timeout.ms", 120_000);
          put("fs.gs.cooperative.locking.max.concurrent.operations", 20);
        }
      };

  @Test
  public void testProxyProperties_throwsExceptionWhenMissingProxyAddress() {
    HadoopConfigurationProperty<String> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_USERNAME.getKey(), "proxy-user");
    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_PASSWORD.getKey(), "proxy-pass");

    Configuration config = new Configuration();
    config.set(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set(gcsProxyPassword.getKey(), gcsProxyPassword.getDefault());
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void testProxyPropertiesAll() {
    HadoopConfigurationProperty<String> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_USERNAME.getKey(), "proxy-user");
    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_PASSWORD.getKey(), "proxy-pass");
    HadoopConfigurationProperty<String> gcsProxyAddress =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_ADDRESS.getKey(), "proxy-address");

    Configuration config = new Configuration();
    config.set(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set(gcsProxyPassword.getKey(), gcsProxyPassword.getDefault());
    config.set(gcsProxyAddress.getKey(), gcsProxyAddress.getDefault());
    GoogleCloudStorageFileSystemOptions options =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    assertThat(options.getCloudStorageOptions().getProxyUsername()).isEqualTo("proxy-user");
    assertThat(options.getCloudStorageOptions().getProxyPassword()).isEqualTo("proxy-pass");
    assertThat(options.getCloudStorageOptions().getProxyAddress()).isEqualTo("proxy-address");
  }

  @Test
  public void testDeprecatedKeys_throwsExceptionWhenDeprecatedKeyIsUsed() {
    HadoopConfigurationProperty<String> gcsProxyAddress =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_ADDRESS.getKey(),
            "proxy-address",
            "fs.gs.proxy.deprecated.address");

    HadoopConfigurationProperty<Integer> gcsProxyUsername =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_USERNAME.getKey(),
            1234,
            "fs.gs.proxy.deprecated.user");

    HadoopConfigurationProperty<String> gcsProxyPassword =
        new HadoopConfigurationProperty<>(
            HadoopCredentialConfiguration.PROXY_PASSWORD.getKey(),
            "proxy-pass",
            "fs.gs.proxy.deprecated.pass");

    Configuration config = new Configuration();
    config.set(gcsProxyAddress.getKey(), gcsProxyAddress.getDefault());
    config.setInt(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set("fs.gs.proxy.deprecated.pass", gcsProxyPassword.getDefault());

    // Verify that we can read password from config when used key is deprecated.
    String userPass = gcsProxyPassword.getPassword(config);
    assertThat(userPass).isEqualTo("proxy-pass");

    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(config);

    // Building configuration using deprecated key (in eg. proxy password) should fail.
    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void defaultPropertiesValues() {
    Arrays.stream(GoogleHadoopFileSystemConfiguration.class.getDeclaredFields())
        .filter(f -> HadoopConfigurationProperty.class.equals(f.getType()))
        .map(GoogleHadoopFileSystemConfigurationTest::getDefaultProperty)
        .forEach(
            p ->
                assertWithMessage("Unexpected default value for '%s' key", p.getKey())
                    .that(p.getDefault())
                    .isEqualTo(expectedDefaultConfiguration.get(p.getKey())));
  }

  private static HadoopConfigurationProperty<?> getDefaultProperty(Field field) {
    try {
      return (HadoopConfigurationProperty) field.get(null);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          String.format("Failed to get '%s' field value", field.getName()), e);
    }
  }
}
