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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    assertEquals("foo.bar", BigQueryStrings.toString(tableRef));

    // Empty string doesn't cause a leading ':'.
    tableRef.setProjectId("");
    assertEquals("foo.bar", BigQueryStrings.toString(tableRef));
  }

  @Test
  public void testTableReferenceToStringWithProjectId() {
    TableReference tableRef = new TableReference()
        .setProjectId("foo-proj")
        .setDatasetId("foo")
        .setTableId("bar");
    assertEquals("foo-proj:foo.bar", BigQueryStrings.toString(tableRef));
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
    assertNull(tableRef.getProjectId());
    assertEquals("fooA1_", tableRef.getDatasetId());
    assertEquals("2bar", tableRef.getTableId());
  }

  @Test
  public void testParseTableReferenceWithProject() {
    // ProjectId itself may contain '.' and ':'.
    TableReference tableRef = BigQueryStrings.parseTableReference("google.com:foo-proj:foo.bar");
    assertEquals("google.com:foo-proj", tableRef.getProjectId());
    assertEquals("foo", tableRef.getDatasetId());
    assertEquals("bar", tableRef.getTableId());
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
