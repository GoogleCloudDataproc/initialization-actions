package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemConfigurationPropertyTest {

  @Test
  public void testPropertyCreation_withNullDeprecationKey() {
    GoogleHadoopFileSystemConfigurationProperty<Integer> newKeyWithoutDeprecatedKey =
        new GoogleHadoopFileSystemConfigurationProperty<>("actual.key", 0, (String[]) null);

    assertThat(newKeyWithoutDeprecatedKey.getDefault()).isEqualTo(0);
  }

  @Test
  public void getStringCollection_throwsExceptionOnNonCollectionProperty()
      throws IllegalArgumentException {
    Configuration config = new Configuration();
    GoogleHadoopFileSystemConfigurationProperty<String> stringKey =
        new GoogleHadoopFileSystemConfigurationProperty<>("actual.key", "default-string");
    GoogleHadoopFileSystemConfigurationProperty<Integer> integerKey =
        new GoogleHadoopFileSystemConfigurationProperty<>("actual.key", 1);
    GoogleHadoopFileSystemConfigurationProperty<Collection<String>> collectionKey =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            "collection.key", ImmutableList.of("key1", "key2"));

    assertThrows(IllegalStateException.class, () -> stringKey.getStringCollection(config));
    assertThrows(IllegalStateException.class, () -> integerKey.getStringCollection(config));
    assertThat(collectionKey.getStringCollection(config)).containsExactly("key1", "key2").inOrder();
  }

  @Test
  public void testProxyProperties_throwsExceptionWhenMissingProxyAddress()
      throws IllegalArgumentException {
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyUsername =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_USERNAME_KEY, "proxy-user");
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyPassword =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_PASSWORD_KEY, "proxy-pass");

    Configuration config = new Configuration();
    config.set(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set(gcsProxyPassword.getKey(), gcsProxyPassword.getDefault());
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);

    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }

  @Test
  public void testProxyPropertiesAll() {
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyUsername =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_USERNAME_KEY, "proxy-user");
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyPassword =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_PASSWORD_KEY, "proxy-pass");
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyAddress =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_ADDRESS_KEY, "proxy-address");

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
  public void testDeprecatedKeys_throwsExceptionWhenDeprecatedKeyIsUsed()
      throws IllegalArgumentException {
    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyAddress =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_ADDRESS_KEY,
            "proxy-address",
            "fs.gs.proxy.deprecated.address");

    GoogleHadoopFileSystemConfigurationProperty<Integer> gcsProxyUsername =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_USERNAME_KEY, 1234, "fs.gs.proxy.deprecated.user");

    GoogleHadoopFileSystemConfigurationProperty<String> gcsProxyPassword =
        new GoogleHadoopFileSystemConfigurationProperty<>(
            EntriesCredentialConfiguration.PROXY_PASSWORD_KEY,
            "proxy-pass",
            "fs.gs.proxy.deprecated.pass");

    Configuration config = new Configuration();
    config.set(gcsProxyAddress.getKey(), gcsProxyAddress.getDefault());
    config.setInt(gcsProxyUsername.getKey(), gcsProxyUsername.getDefault());
    config.set("fs.gs.proxy.deprecated.pass", gcsProxyPassword.getDefault());

    // Verify that we can read password from config when used key is deprecated.
    String userPass = gcsProxyPassword.getPassword(config);
    assertThat(userPass).isEqualTo("proxy-pass");

    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);

    // Building configuration using deprecated key (in eg. proxy password) should fail.
    assertThrows(IllegalArgumentException.class, optionsBuilder::build);
  }
}
