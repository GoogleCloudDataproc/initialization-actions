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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests for JSON BigQuery exports
 */
@RunWith(Parameterized.class)
public class GsonBigQueryIoIntegrationTest extends
    AbstractBigQueryIoIntegrationTestBase<JsonObject> {

  @Parameterized.Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    return Arrays.asList(new Object[][]{
        {true},
        {false}
    });
  }

  public GsonBigQueryIoIntegrationTest(Boolean enableAsyncWrites) {
    super(enableAsyncWrites, new GsonBigQueryInputFormat());
  }

  @Override
  protected Map<String, Object> readReacord(RecordReader<?, JsonObject> recordReader)
      throws IOException, InterruptedException {
    Map<String, Object> result = new HashMap<>();
    JsonObject currentValue = recordReader.getCurrentValue();
    for (Map.Entry<String, JsonElement> entry : currentValue.entrySet()) {
      String key = entry.getKey();
      JsonPrimitive primitiveValue = entry.getValue().getAsJsonPrimitive();
      Object value;
      if (COMPANY_NAME_FIELD_NAME.equals(key)) {
        value = primitiveValue.getAsString();
      } else if (MARKET_CAP_FIELD_NAME.equals(key)) {
        value = primitiveValue.getAsInt();
      } else {
        throw new IllegalStateException(
            String.format("Cannot handle key %s", key));
      }
      result.put(key, value);
    }
    return result;
  }
}
