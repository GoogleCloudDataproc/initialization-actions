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

/**
 * BigQuery export file formats
 */
public enum ExportFileFormat {
  /**
   * Comma separated value exports
   */
  CSV("data-*.csv", "CSV"),
  /**
   * Newline delimited JSON
   */
  LINE_DELIMITED_JSON("data-*.json", "NEWLINE_DELIMITED_JSON"),
  /**
   * Avro container files
   */
  AVRO("data-*.avro", "AVRO");

  private final String filePattern;
  private final String formatIdentifier;

  private ExportFileFormat(String filePattern, String formatIdentifier) {
    this.filePattern = filePattern;
    this.formatIdentifier = formatIdentifier;
  }

  /**
   * Get the file pattern to use when exporting
   */
  public String getFilePattern() {
    return filePattern;
  }

  /**
   * Get the identifier to specify in API requests
   */
  public String getFormatIdentifier() {
    return formatIdentifier;
  }
}
