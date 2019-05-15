/*
 * Copyright 2019 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery.output;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryTimePartitioningTest {

  public static final String TIME_PARTITIONING_JSON =
      "{\"expirationMs\":\"1000\",\"field\":\"ingestDate\",\"requirePartitionFilter\":true,"
          + "\"type\":\"DAY\"}";

  @Test
  public void testConvertToJson() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("DAY");
    bigQueryTimePartitioning.setExpirationMs(1000L);
    bigQueryTimePartitioning.setField("ingestDate");
    bigQueryTimePartitioning.setRequirePartitionFilter(true);

    assertThat(bigQueryTimePartitioning.getAsJson()).isEqualTo(TIME_PARTITIONING_JSON);
  }

  @Test
  public void testConvertFromJson() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("DAY");
    bigQueryTimePartitioning.setExpirationMs(1000L);
    bigQueryTimePartitioning.setField("ingestDate");
    bigQueryTimePartitioning.setRequirePartitionFilter(true);

    assertThat(BigQueryTimePartitioning.getFromJson(TIME_PARTITIONING_JSON))
        .isEqualTo(bigQueryTimePartitioning.get());
  }

  @Test
  public void testConversion_OnlyTypeIsPresent() throws IOException {
    BigQueryTimePartitioning bigQueryTimePartitioning = new BigQueryTimePartitioning();
    bigQueryTimePartitioning.setType("DAY");
    String json = bigQueryTimePartitioning.getAsJson();

    assertThat(json).isEqualTo("{\"type\":\"DAY\"}");
    assertThat(BigQueryTimePartitioning.getFromJson(json).getType()).isEqualTo("DAY");
  }
}
