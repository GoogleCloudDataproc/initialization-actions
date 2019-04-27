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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryTableSchemaTest {

  @Test
  public void testBigQueryTableSchema() throws IOException {
    ImmutableList<BigQueryTableFieldSchema> fields =
        ImmutableList.of(
            new BigQueryTableFieldSchema().setName("Field1").setType("STRING"),
            new BigQueryTableFieldSchema().setName("Field2").setType("INTEGER"),
            new BigQueryTableFieldSchema()
                .setName("Field3")
                .setType("RECORD")
                .setFields(
                    ImmutableList.of(
                        new BigQueryTableFieldSchema().setName("NestedField1").setType("STRING"),
                        new BigQueryTableFieldSchema()
                            .setName("NestedField2")
                            .setType("INTEGER"))));
    BigQueryTableSchema tableSchema = new BigQueryTableSchema().setFields(fields);
    String json = BigQueryTableHelper.getTableSchemaJson(tableSchema.get());
    String expectedJson =
        "{\"fields\":[{\"name\":\"Field1\",\"type\":\"STRING\"},"
            + "{\"name\":\"Field2\",\"type\":\"INTEGER\"},"
            + "{\"fields\":[{\"name\":\"NestedField1\",\"type\":\"STRING\"},"
            + "{\"name\":\"NestedField2\",\"type\":\"INTEGER\"}],"
            + "\"name\":\"Field3\",\"type\":\"RECORD\"}]}";
    assertThat(json).isEqualTo(expectedJson);
  }
}
