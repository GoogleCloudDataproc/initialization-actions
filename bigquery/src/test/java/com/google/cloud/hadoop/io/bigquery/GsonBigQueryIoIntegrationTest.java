package com.google.cloud.hadoop.io.bigquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
