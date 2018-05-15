/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * CredentialConfiguration based on Hadoop Configuration objects.
 *
 * When reading configuration this class makes use of a list of key prefixes that are each applied
 * to key suffixes to create a complete configuration key. There is a base prefix of 'google.cloud.'
 * that is included by the builder for each HadoopCredentialConfiguration created. When
 * constructing, other prefixes can be specified. Prefixes specified later can be used to override
 * the values of previously set values. In this way a set of global credentials can be specified
 * for most connectors with an override specified for any connectors that need different
 * credentials.
 */
public class HadoopCredentialConfiguration
    extends EntriesCredentialConfiguration implements Configurable {

  /**
   * An adapter to use our Configuration object as the config object
   * for our superclass.
   */
  public static class ConfigurationEntriesAdapter
      implements EntriesCredentialConfiguration.Entries {
    Configuration config;

    /// Create our adapter.
    public ConfigurationEntriesAdapter(Configuration config) {
      this.config = config;
    }

    public String get(String key) {
      return config.get(key);
    }

    public void set(String key, String value) {
      config.set(key, value);
    }

    public void setBoolean(String key, boolean value) {
      config.setBoolean(key, value);
    }
  }

  /**
   * Create a Builder with a withConfiguration method that
   * takes our Configuration.
   */
  public static class Builder
      extends EntriesCredentialConfiguration.Builder
      <HadoopCredentialConfiguration.Builder, HadoopCredentialConfiguration> {
    public Builder withConfiguration(Configuration config) {
      super.withConfiguration(new ConfigurationEntriesAdapter(config));
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected HadoopCredentialConfiguration beginBuild() {
      return new HadoopCredentialConfiguration(prefixes);
    }
  }

  public HadoopCredentialConfiguration(List<String> prefixes) {
    super(prefixes);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Translates the "Entries" configuration of the superclass into a Hadoop "Configuration"
   * using getConfigurationInto() on a wrapper which passes through each key from the Entries
   * to the corresponding Hadoop object.
   */
  public Configuration getConf() {
    Configuration configuration = new Configuration();
    getConfigurationInto(new ConfigurationEntriesAdapter(configuration));
    return configuration;
  }

  /**
   * Load configuration values from the provided Configuration source. For any key that does not
   * have a corresponding value in the Configuration, no changes will be made to the state of this
   * object.
   */
  public void setConf(Configuration entries) {
    setConfiguration(new ConfigurationEntriesAdapter(entries));
  }
}
