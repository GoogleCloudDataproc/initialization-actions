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

package com.google.cloud.hadoop.util;

import org.apache.hadoop.conf.Configuration;

/**
 * A generic adapter to use a Hadoop Configuration object as the config object for our superclass.
 */
public abstract class ConfigurationEntriesAdapterGeneric
    implements EntriesCredentialConfiguration.Entries {
  protected Configuration config;

  /// Create our adapter.
  public ConfigurationEntriesAdapterGeneric(Configuration config) {
    this.config = config;
  }

  @Override
  public String get(String key) {
    return config.get(key);
  }

  @Override
  public void set(String key, String value) {
    config.set(key, value);
  }

  @Override
  public void setBoolean(String key, boolean value) {
    config.setBoolean(key, value);
  }
}
