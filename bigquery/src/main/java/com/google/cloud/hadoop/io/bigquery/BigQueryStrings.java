/*
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

import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.util.List;

/**
 * BigQueryStrings provides misc static helper methods for printing and formatting strings related
 * to BigQuery data structures and API objects.
 */
public class BigQueryStrings {
  // Regular expression for validating a datasetId.tableId pair.
  public static final String DATASET_AND_TABLE_REGEX = "[a-zA-Z0-9_]+\\.[a-zA-Z0-9_$]+";

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  /**
   * Returns a String representation of the TableReference suitable for interop with other bigquery
   * tools and for passing back into {@link #parseTableReference(String)}.
   *
   * @param tableRef A TableReference which contains at least DatasetId and TableId.
   * @return A string of the form [projectId]:[datasetId].[tableId].
   */
  public static String toString(TableReference tableRef) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getDatasetId()),
        "tableRef must contain non-empty DatasetId.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getTableId()),
        "tableRef must contain non-empty TableId.");
    if (Strings.isNullOrEmpty(tableRef.getProjectId())) {
      return String.format("%s.%s", tableRef.getDatasetId(), tableRef.getTableId());
    } else {
      return String.format("%s:%s.%s",
          tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId());
    }
  }

  /**
   * Parses a string into a TableReference; projectId may be omitted if the caller defines a
   * "default" project; in such a case, getProjectId() of the returned TableReference will
   * return null.
   *
   * @param tableRefString A string of the form [projectId]:[datasetId].[tableId].
   * @return a TableReference with the parsed components.
   */
  public static TableReference parseTableReference(String tableRefString) {
    // Logic mirrored from cloud/helix/clients/cli/bigquery_client.py.
    TableReference tableRef = new TableReference();
    int projectIdEnd = tableRefString.lastIndexOf(':');
    String datasetAndTableString = tableRefString;
    if (projectIdEnd != -1) {
      tableRef.setProjectId(tableRefString.substring(0, projectIdEnd));

      // Omit the ':' from the remaining datasetId.tableId substring.
      datasetAndTableString = tableRefString.substring(projectIdEnd + 1);
    }

    Preconditions.checkArgument(datasetAndTableString.matches(DATASET_AND_TABLE_REGEX),
        "Invalid datasetAndTableString '%s'; must match regex '%s'.",
        datasetAndTableString, DATASET_AND_TABLE_REGEX);

    List<String> idParts = DOT_SPLITTER.splitToList(datasetAndTableString);
    tableRef.setDatasetId(idParts.get(0));
    tableRef.setTableId(idParts.get(1));
    return tableRef;
  }
}
