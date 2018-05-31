/*
 * Copyright 2018 Google LLC
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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Helper for BigQuery tables. */
public class BigQueryTableHelper {
  /**
   * Parses table schema JSON into {@link TableSchema}.
   *
   * @param tableSchemaJson
   * @return {@link TableSchema}
   * @throws IOException if the JSON is invalid.
   */
  public static TableSchema parseTableSchema(String tableSchemaJson) throws IOException {
    JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(tableSchemaJson);
    return parser.parseAndClose(TableSchema.class);
  }

  /**
   * Creates {@link TableSchema} from the JSON representation of the table fields.
   *
   * @param fieldsJson
   * @return {@link TableSchema}
   * @throws IOException
   */
  public static TableSchema createTableSchemaFromFields(String fieldsJson) throws IOException {
    List<TableFieldSchema> fields = new ArrayList<>();
    JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(fieldsJson);
    parser.parseArrayAndClose(fields, TableFieldSchema.class);

    return new TableSchema().setFields(fields);
  }

  /**
   * Gets the JSON representation of the table schema.
   *
   * @param tableSchema
   * @return the JSON of the table schema.
   * @throws IOException
   */
  public static String getTableSchemaJson(TableSchema tableSchema) throws IOException {
    return JacksonFactory.getDefaultInstance().toString(tableSchema);
  }

  /**
   * Gets the JSON representation of the table's fields.
   *
   * @param tableSchema
   * @return the JSON of the fields.
   * @throws IOException
   */
  public static String getTableFieldsJson(TableSchema tableSchema) throws IOException {
    return JacksonFactory.getDefaultInstance().toString(tableSchema.getFields());
  }
}
