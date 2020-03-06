package com.google.cloud.hadoop.fs.gcs;

/** Hadoop configuration property for Google Cloud Storage Connector */
public class HadoopConfigurationProperty<T>
    extends com.google.cloud.hadoop.util.HadoopConfigurationProperty<T> {

  public HadoopConfigurationProperty(String key) {
    super(key);
  }

  public HadoopConfigurationProperty(String key, T defaultValue, String... deprecatedKeys) {
    super(key, defaultValue, deprecatedKeys);
  }
}
