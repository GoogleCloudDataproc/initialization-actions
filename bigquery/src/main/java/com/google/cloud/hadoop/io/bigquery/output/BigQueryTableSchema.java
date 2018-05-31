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
package com.google.cloud.hadoop.io.bigquery.output;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for BigQuery {@link TableSchema}.
 *
 * <p>This class is used to avoid client code to depend on BigQuery API classes, so that there is no
 * potential conflict between different versions of BigQuery API libraries in the client.
 *
 * @see TableSchema.
 */
public class BigQueryTableSchema {
  private final TableSchema tableSchema;

  public BigQueryTableSchema() {
    this.tableSchema = new TableSchema();
  }

  BigQueryTableSchema(TableSchema tableSchema) {
    Preconditions.checkNotNull(tableSchema, "tableSchema is null.");
    this.tableSchema = tableSchema;
  }

  /** @see TableSchema#setFields(List) */
  public BigQueryTableSchema setFields(List<BigQueryTableFieldSchema> bigQueryTableFields) {
    Preconditions.checkArgument(!bigQueryTableFields.isEmpty(), "Empty fields.");
    List<TableFieldSchema> fields = new ArrayList<>(bigQueryTableFields.size());
    for (BigQueryTableFieldSchema bigQueryTableField : bigQueryTableFields) {
      fields.add(bigQueryTableField.get());
    }
    tableSchema.setFields(fields);
    return this;
  }

  @Override
  public int hashCode() {
    return tableSchema.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof BigQueryTableSchema)) {
      return false;
    }
    BigQueryTableSchema another = (BigQueryTableSchema) object;
    return tableSchema.equals(another.tableSchema);
  }

  TableSchema get() {
    return tableSchema;
  }

  static BigQueryTableSchema wrap(TableSchema tableSchema) {
    return new BigQueryTableSchema(tableSchema);
  }
}
