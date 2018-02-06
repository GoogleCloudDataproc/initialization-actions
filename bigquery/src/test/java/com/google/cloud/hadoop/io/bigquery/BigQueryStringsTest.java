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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unittests for BigQueryStrings formatting/parsing of BigQuery-related data structures.
 */
@RunWith(JUnit4.class)
public class BigQueryStringsTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTableReferenceToStringWithNoProjectId() {
    TableReference tableRef = new TableReference()
        .setDatasetId("foo")
        .setTableId("bar");
    assertThat(BigQueryStrings.toString(tableRef)).isEqualTo("foo.bar");

    // Empty string doesn't cause a leading ':'.
    tableRef.setProjectId("");
    assertThat(BigQueryStrings.toString(tableRef)).isEqualTo("foo.bar");
  }

  @Test
  public void testTableReferenceToStringWithProjectId() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId("foo")
        .setTableId("bar");
    assertThat(BigQueryStrings.toString(tableRef)).isEqualTo("foo-proj:foo.bar");
  }

  @Test
  public void testToStringThrowsWhenTableIdIsNull() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId("foo")
        .setTableId(null);
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.toString(tableRef);
  }

  @Test
  public void testToStringThrowsWhenTableIdIsEmpty() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId("foo")
        .setTableId("");
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.toString(tableRef);
  }

  @Test
  public void testToStringThrowsWhenDatasetIsNull() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId(null)
        .setTableId("tableId");
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.toString(tableRef);
  }

  @Test
  public void testToStringThrowsWhenDatasetIsEmpty() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId("")
        .setTableId("tableId");
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.toString(tableRef);
  }

  @Test
  public void testParseTableReferenceNoProject() {
    TableReference tableRef = BigQueryStrings.parseTableReference("fooA1_.2bar");
    assertThat(tableRef.getProjectId()).isNull();
    assertThat(tableRef.getDatasetId()).isEqualTo("fooA1_");
    assertThat(tableRef.getTableId()).isEqualTo("2bar");
  }

  @Test
  public void testParseTableReferenceWithProject() {
    // ProjectId itself may contain '.' and ':'.
    TableReference tableRef = BigQueryStrings.parseTableReference("google.com:foo-proj:foo.bar");
    assertThat(tableRef.getProjectId()).isEqualTo("google.com:foo-proj");
    assertThat(tableRef.getDatasetId()).isEqualTo("foo");
    assertThat(tableRef.getTableId()).isEqualTo("bar");
  }

  @Test
  public void testParseTableReferenceThrowsWhenDashesArePresent() {
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.parseTableReference("foo-o.bar");
  }

  @Test
  public void testParseTableReferenceThrowsWhenNoDotsPresent() {
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.parseTableReference("foo");
  }

  @Test
  public void testParseTableReferenceThrowsWhenOnlyOneDotPresent() {
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.parseTableReference("p.foo:bar");

  }

  @Test
  public void testParseTableReferenceThrowsWhenDatasetIsEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.parseTableReference("foo.");
  }

  @Test
  public void testParseTableReferenceThrowsWhenTableIsEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    BigQueryStrings.parseTableReference(".bar");
  }
}
