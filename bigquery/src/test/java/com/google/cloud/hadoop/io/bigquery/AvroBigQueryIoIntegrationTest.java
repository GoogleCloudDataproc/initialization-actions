/**
 * Copyright 2017 Google LLC
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
package com.google.cloud.hadoop.io.bigquery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests for Avro BigQuery exports
 */
@RunWith(Parameterized.class)
public class AvroBigQueryIoIntegrationTest extends
    AbstractBigQueryIoIntegrationTestBase<GenericData.Record> {

  @Parameterized.Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    return Arrays.asList(new Object[][]{
        {true},
        {false}
    });
  }

  public AvroBigQueryIoIntegrationTest(Boolean enableAsyncWrites) {
    super(enableAsyncWrites, new AvroBigQueryInputFormat());
  }

  @Override
  protected Map<String, Object> readReacord(RecordReader<?, GenericData.Record> recordReader)
      throws IOException, InterruptedException {
    Map<String, Object> result = new HashMap<>();
    GenericData.Record currentValue = recordReader.getCurrentValue();
    Schema schema = currentValue.getSchema();
    for (Schema.Field field : schema.getFields()) {
      if (COMPANY_NAME_FIELD_NAME.equals(field.name())) {
        // String data comes in as org.apache.avro.util.Utf8, need to convert to java string:
        result.put(
            COMPANY_NAME_FIELD_NAME,
            currentValue.get(COMPANY_NAME_FIELD_NAME).toString());
      } else if (MARKET_CAP_FIELD_NAME.equals(field.name())) {
        result.put(
            MARKET_CAP_FIELD_NAME,
            ((Long) currentValue.get(MARKET_CAP_FIELD_NAME)).intValue());
      } else {
        throw new IllegalStateException(
            String.format("Don't know how to handle field %s", field.name()));
      }
    }
    return result;
  }
}
