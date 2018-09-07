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

package com.google.cloud.hadoop.testing;

import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration.Entries;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for creating configuration objects for use in testing.
 */
public class EntriesCredentialConfigurationUtil {
  public static void addTestConfigurationSettings(Entries configuration) {
    configuration.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX
        + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
            "false");

    configuration.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX
        + EntriesCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX, "true");
  }

  public static Entries getTestConfiguration() {
    Entries configuration = new TestEntries();
    addTestConfigurationSettings(configuration);
    return configuration;
  }

  /**
   * A class to hold Entries for EntriesCredentialConfiguration for testing.
   */
  public static class TestEntries implements Entries {
    private Map<String, Object> map = new HashMap<String, Object>();

    public void set(String key, String value) {
      map.put(key, value);
    }

    public String get(String key) {
      Object v = map.get(key);
      return (v == null) ? null : v.toString();
    }

    public void setBoolean(String key, boolean value) {
      Boolean b = Boolean.valueOf(value);
      map.put(key, b);
    }
  }
}
