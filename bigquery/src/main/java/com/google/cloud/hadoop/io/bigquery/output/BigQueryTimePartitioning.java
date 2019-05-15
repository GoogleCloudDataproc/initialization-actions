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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.IOException;

/**
 * Wrapper for BigQuery {@link TimePartitioning}.
 *
 * <p>This class is used to avoid client code to depend on BigQuery API classes, so that there is no
 * potential conflict between different versions of BigQuery API libraries in the client.
 *
 * @see TimePartitioning.
 */
public class BigQueryTimePartitioning {
  private final TimePartitioning timePartitioning;

  public BigQueryTimePartitioning() {
    this.timePartitioning = new TimePartitioning();
  }

  public BigQueryTimePartitioning(TimePartitioning timePartitioning) {
    this.timePartitioning = timePartitioning;
  }

  public String getType() {
    return timePartitioning.getType();
  }

  public void setType(String type) {
    timePartitioning.setType(type);
  }

  public String getField() {
    return timePartitioning.getField();
  }

  public void setField(String field) {
    timePartitioning.setField(field);
  }

  public long getExpirationMs() {
    return timePartitioning.getExpirationMs();
  }

  public void setExpirationMs(long expirationMs) {
    timePartitioning.setExpirationMs(expirationMs);
  }

  public Boolean getRequirePartitionFilter() {
    return timePartitioning.getRequirePartitionFilter();
  }

  public void setRequirePartitionFilter(Boolean requirePartitionFilter) {
    timePartitioning.setRequirePartitionFilter(requirePartitionFilter);
  }

  @Override
  public int hashCode() {
    return timePartitioning.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BigQueryTimePartitioning)) {
      return false;
    }
    BigQueryTimePartitioning other = (BigQueryTimePartitioning) obj;
    return timePartitioning.equals(other.timePartitioning);
  }

  TimePartitioning get() {
    return timePartitioning;
  }

  static TimePartitioning getFromJson(String json) throws IOException {
    JsonParser parser = JacksonFactory.getDefaultInstance().createJsonParser(json);
    return parser.parseAndClose(TimePartitioning.class);
  }

  public String getAsJson() throws IOException {
    return JacksonFactory.getDefaultInstance().toString(timePartitioning);
  }

  static BigQueryTimePartitioning wrap(TimePartitioning tableTimePartitioning) {
    return new BigQueryTimePartitioning(tableTimePartitioning);
  }
}
