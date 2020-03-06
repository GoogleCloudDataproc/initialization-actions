package com.google.cloud.hadoop.io.bigquery;

/** Hadoop configuration property for BigQuery Connector */
public class HadoopConfigurationProperty<T>
    extends com.google.cloud.hadoop.util.HadoopConfigurationProperty<T> {

  public HadoopConfigurationProperty(String key) {
    super(key);
  }

  public HadoopConfigurationProperty(String key, T defaultValue, String... deprecatedKeys) {
    super(key, defaultValue, deprecatedKeys);
  }
}
