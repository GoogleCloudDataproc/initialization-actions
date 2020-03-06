/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.util.testing;

import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Utils for Hadoop configuration testing */
public class HadoopConfigurationUtils {

  public static Map<String, ?> getDefaultProperties(Class<?> configurationClass) {
    return Arrays.stream(configurationClass.getDeclaredFields())
        .filter(f -> HadoopConfigurationProperty.class.isAssignableFrom(f.getType()))
        .map(HadoopConfigurationUtils::getDefaultProperty)
        .collect(
            HashMap::new,
            (map, p) -> {
              for (String deprecatedKey : p.getDeprecatedKeys()) {
                map.put(deprecatedKey, p.getDefault());
              }
              map.put(p.getKey(), p.getDefault());
            },
            HashMap::putAll);
  }

  private static HadoopConfigurationProperty<?> getDefaultProperty(Field field) {
    try {
      return (HadoopConfigurationProperty) field.get(null);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          String.format("Failed to get '%s' field value", field.getName()), e);
    }
  }

  private HadoopConfigurationUtils() {}
}
