/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Helpers for interacting with properties files
 */
public class PropertyUtil {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Get the value of a property or a default value if there's an error retrieving the property key.
   * @param referenceClass The class which should be used to find the property file. The property
   * file is expected to be packaged in the same directory as this class.
   * @param propertyFile The name of the property file to be read.
   * @param key The property key to find in the property file.
   * @param defaultValue The value to return if no property with the given key is found or if the
   * property file cannot be found.
   * @return The value specified in the property file or defaultValue if an error occurs or if the
   * key could not be found
   */
  public static String getPropertyOrDefault(
      Class<?> referenceClass,
      String propertyFile,
      String key,
      String defaultValue) {
    try (InputStream stream = referenceClass.getResourceAsStream(propertyFile)) {
      if (stream == null) {
        logger.atSevere().log("Could not load properties file '%s'", propertyFile);
        return defaultValue;
      }
      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty(key);
      if (value == null) {
        logger.atSevere().log("Key %s not found in properties file %s.", key, propertyFile);
        return defaultValue;
      }
      return value;
    } catch (IOException e) {
      logger.atSevere().withCause(e).log(
          "Error while trying to get property value for key %s", key);
      return defaultValue;
    }
  }
}
