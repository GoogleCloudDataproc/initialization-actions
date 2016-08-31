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
