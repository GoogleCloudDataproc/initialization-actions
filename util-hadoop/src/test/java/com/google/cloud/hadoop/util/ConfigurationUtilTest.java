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

import static com.google.cloud.hadoop.util.ConfigurationUtil.getMandatoryConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for ConfigurationUtil class.
 */
@RunWith(JUnit4.class)
public class ConfigurationUtilTest {
  private static final HadoopConfigurationProperty<String> PROPERTY_ONE =
      new HadoopConfigurationProperty<>("test-key");
  private static final String VALUE_ONE = "test-value";
  private static final HadoopConfigurationProperty<String> PROPERTY_TWO =
      new HadoopConfigurationProperty<>("test-key2");
  private static final String VALUE_TWO = "test-value2";

  /**
   * Verifies getMandatoryConfig method for single strings.
   */
  @Test
  public void testSingleStringGetMandatoryConfig() throws IOException {
    // Test null value.
    Configuration config = new Configuration();
    assertThrows(IOException.class, () -> getMandatoryConfig(config, PROPERTY_ONE));

    // Test empty string.
    config.set(PROPERTY_ONE.getKey(), "");
    assertThrows(IOException.class, () -> getMandatoryConfig(config, PROPERTY_ONE));

    // Test proper setting.
    config.set(PROPERTY_ONE.getKey(), VALUE_ONE);
    assertThat(getMandatoryConfig(config, PROPERTY_ONE)).isEqualTo(VALUE_ONE);
  }

  /**
   * Verifies getMandatoryConfig method for a list of strings.
   */
  @Test
  public void testListGetMandatoryConfig() throws IOException {
    // Test one null value.
    Configuration config = new Configuration();
    config.set(PROPERTY_ONE.getKey(), VALUE_ONE);

    assertThrows(
        IOException.class,
        () -> getMandatoryConfig(config, Lists.newArrayList(PROPERTY_ONE, PROPERTY_TWO)));

    // Test one empty string.
    config.set(PROPERTY_TWO.getKey(), "");
    assertThrows(
        IOException.class,
        () -> getMandatoryConfig(config, Lists.newArrayList(PROPERTY_ONE, PROPERTY_TWO)));

    // Test proper setting.
    config.set(PROPERTY_TWO.getKey(), VALUE_TWO);
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put(PROPERTY_ONE.getKey(), VALUE_ONE);
    expectedMap.put(PROPERTY_TWO.getKey(), VALUE_TWO);

    assertThat(getMandatoryConfig(config, Lists.newArrayList(PROPERTY_ONE, PROPERTY_TWO)))
        .isEqualTo(expectedMap);
  }
}
