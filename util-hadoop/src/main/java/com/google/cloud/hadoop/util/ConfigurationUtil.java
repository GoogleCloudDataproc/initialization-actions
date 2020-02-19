/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Helpers for checking the validity of Hadoop configurations.
 */
public class ConfigurationUtil {

  /** Gets value for the given key or throws if value is not found. */
  public static String getMandatoryConfig(
      Configuration config, HadoopConfigurationProperty<?> property) throws IOException {
    String value = config.get(property.getKey());
    if (Strings.isNullOrEmpty(value)) {
      throw new IOException("Must supply a value for configuration setting: " + property.getKey());
    }
    return value;
  }

  /** Gets value for the given keys or throws if one or more values are not found. */
  public static Map<String, String> getMandatoryConfig(
      Configuration config, List<HadoopConfigurationProperty<?>> properties) throws IOException {
    List<String> missingKeys = new ArrayList<>();
    Map<String, String> values = new HashMap<>();
    for (HadoopConfigurationProperty<?> property : properties) {
      String value = config.get(property.getKey());
      if (Strings.isNullOrEmpty(value)) {
        missingKeys.add(property.getKey());
      } else {
        values.put(property.getKey(), value);
      }
    }
    if (missingKeys.size() > 0) {
      Joiner joiner = Joiner.on(", ");
      String message = "Must supply value for configuration settings: " + joiner.join(missingKeys);
      throw new IOException(message);
    }
    return values;
  }
}
