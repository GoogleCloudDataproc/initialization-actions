/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopConfigurationPropertyTest {

  @Test
  public void testPropertyCreation_withNullDeprecationKey() {
    HadoopConfigurationProperty<Integer> newKeyWithoutDeprecatedKey =
        new HadoopConfigurationProperty<>("actual.key", 0, (String[]) null);

    assertThat(newKeyWithoutDeprecatedKey.getDefault()).isEqualTo(0);
  }

  @Test
  public void getStringCollection_throwsExceptionOnNonCollectionProperty() {
    Configuration config = new Configuration();
    HadoopConfigurationProperty<String> stringKey =
        new HadoopConfigurationProperty<>("actual.key", "default-string");
    HadoopConfigurationProperty<Integer> integerKey =
        new HadoopConfigurationProperty<>("actual.key", 1);
    HadoopConfigurationProperty<Collection<String>> collectionKey =
        new HadoopConfigurationProperty<>("collection.key", ImmutableList.of("key1", "key2"));

    assertThrows(IllegalStateException.class, () -> stringKey.getStringCollection(config));
    assertThrows(IllegalStateException.class, () -> integerKey.getStringCollection(config));
    assertThat(collectionKey.getStringCollection(config)).containsExactly("key1", "key2").inOrder();
  }
}
