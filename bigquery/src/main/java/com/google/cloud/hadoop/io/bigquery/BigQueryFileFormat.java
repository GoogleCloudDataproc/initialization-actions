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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceStability;

/** An enum to describe file formats supported by the BigQuery api. */
@InterfaceStability.Unstable
public enum BigQueryFileFormat {

  /** Comma separated value exports */
  CSV(".csv", "CSV"),

  /** Newline delimited JSON */
  NEWLINE_DELIMITED_JSON(".json", "NEWLINE_DELIMITED_JSON"),

  /** Avro container files */
  AVRO(".avro", "AVRO");

  /** Map used for simple deserialization of strings into BigQueryFileFormats. */
  private static final Map<String, BigQueryFileFormat> NAMES_MAP =
      new HashMap<String, BigQueryFileFormat>();

  /** A formatted string of the accepted file formats. */
  private static final String ACCEPTED_FORMATS;

  static {
    List<String> formats = new ArrayList<String>();
    for (BigQueryFileFormat format : BigQueryFileFormat.values()) {
      NAMES_MAP.put(format.name(), format);
      formats.add(format.name());
    }

    ACCEPTED_FORMATS = Joiner.on(',').join(formats);
  }

  private final String extension;
  private final String formatIdentifier;

  private BigQueryFileFormat(String extension, String formatIdentifier) {
    this.extension = extension;
    this.formatIdentifier = formatIdentifier;
  }

  /** Get the default extension to denote the file format. */
  public String getExtension() {
    return extension;
  }

  /** Get the identifier to specify in API requests. */
  public String getFormatIdentifier() {
    return formatIdentifier;
  }

  /**
   * Deserializes the name of a BigQueryFileFormat. If there is no matching BigQueryFileFormat for
   * the given name, an IllegalArugmentException is thrown.
   *
   * @param name the name of the BigQueryFileFormat as returned from {@link
   *     BigQueryFileFormat#name()}.
   * @return the associated BigQueryFileFormat.
   */
  public static BigQueryFileFormat fromName(String name) {
    BigQueryFileFormat entry = NAMES_MAP.get(name);
    if (entry == null) {
      throw new IllegalArgumentException(
          "Unable to find BigQueryFileFormat for '"
              + name
              + "'. Accepted formats are: [ "
              + ACCEPTED_FORMATS
              + " ]");
    }
    return entry;
  }
}
