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
package com.google.cloud.hadoop.io.bigquery.output;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class BigQueryOutputConfigurationTest {
  /** Default projectId for the configuration. */
  private static final String DEFAULT_PROJECT_ID = "google.com:my-project";

  /** Sample projectId for the configuration. */
  private static final String TEST_PROJECT_ID = "domain:project";

  /** Sample datasetId for the configuration. */
  private static final String TEST_DATASET_ID = "dataset";

  /** Sample tableId for the configuration. */
  private static final String TEST_TABLE_ID = "table";

  /** Qualified table ID. */
  private static final String QUALIFIED_TABLE_ID =
      String.format("%s:%s.%s", TEST_PROJECT_ID, TEST_DATASET_ID, TEST_TABLE_ID);

  /** Qualified table ID. */
  private static final String QUALIFIED_TABLE_ID_WITHOUT_PROJECT =
      String.format("%s.%s", TEST_DATASET_ID, TEST_TABLE_ID);

  /** Sample table reference. */
  private static final TableReference TEST_TABLE_REF =
      new TableReference()
          .setProjectId(TEST_PROJECT_ID)
          .setDatasetId(TEST_DATASET_ID)
          .setTableId(TEST_TABLE_ID);

  /** Sample raw output path for data. */
  private static final String TEST_OUTPUT_PATH_STRING = "gs://test_bucket/test_directory";

  /** Sample output file format for the configuration. */
  private static final BigQueryFileFormat TEST_FILE_FORMAT =
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON;

  /** Sample output format class for the configuration. */
  @SuppressWarnings("rawtypes")
  private static final Class<? extends FileOutputFormat> TEST_OUTPUT_CLASS = TextOutputFormat.class;

  /** Sample table schema used for output. */
  private static final BigQueryTableSchema TEST_TABLE_SCHEMA =
      new BigQueryTableSchema()
          .setFields(
              ImmutableList.of(
                  new BigQueryTableFieldSchema().setName("A").setType("STRING"),
                  new BigQueryTableFieldSchema().setName("B").setType("INTEGER")));

  /** Sample expected serialized version of TEST_TABLE_SCHEMA. */
  private static final String TEST_TABLE_SCHEMA_STRING =
      "[{\"name\":\"A\",\"type\":\"STRING\"},{\"name\":\"B\",\"type\":\"INTEGER\"}]";

  /** Sample malformed serialized version of TEST_TABLE_SCHEMA. */
  private static final String TEST_BAD_TABLE_SCHEMA_STRING =
      "[{\"name\":\"A\",\"type\":\"STRING\"},{'name':\"B\",\"type\":\"INTEGER\"}]";

  /** The Job Configuration for testing. */
  private static Configuration conf;

  @Mock private JobID mockJobID;

  /** Set up before all classes. */
  @Before
  public void setUp() throws IOException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    // Create the configuration.
    conf = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();

    new BigQueryOutputConfiguration();
  }

  /** Test the configure function correctly sets the configuration keys. */
  @Test
  public void testConfigure() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);

    assertThat(conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY), is(TEST_PROJECT_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY), is(TEST_DATASET_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY), is(TEST_TABLE_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY), is(TEST_FILE_FORMAT.name()));
    assertThat(
        conf.get(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY), is(TEST_OUTPUT_CLASS.getName()));
    assertThat(
        conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY), is(TEST_TABLE_SCHEMA_STRING));
    assertThat(
        BigQueryOutputConfiguration.getGcsOutputPath(conf).toString(), is(TEST_OUTPUT_PATH_STRING));
  }

  /** Test the configure function correctly sets the configuration keys. */
  @Test
  public void testConfigureWithDefaultProject() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, DEFAULT_PROJECT_ID);
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID_WITHOUT_PROJECT,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);

    assertThat(conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY), is(DEFAULT_PROJECT_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY), is(TEST_DATASET_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY), is(TEST_TABLE_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY), is(TEST_FILE_FORMAT.name()));
    assertThat(
        conf.get(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY), is(TEST_OUTPUT_CLASS.getName()));
    assertThat(
        conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY), is(TEST_TABLE_SCHEMA_STRING));
    assertThat(
        BigQueryOutputConfiguration.getGcsOutputPath(conf).toString(), is(TEST_OUTPUT_PATH_STRING));
  }

  /** Test the configure function correctly sets the configuration keys. */
  @Test
  public void testConfigureWithAutoSchema() throws IOException {
    BigQueryOutputConfiguration.configureWithAutoSchema(
        conf, QUALIFIED_TABLE_ID, TEST_OUTPUT_PATH_STRING, TEST_FILE_FORMAT, TEST_OUTPUT_CLASS);

    assertThat(conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY), is(TEST_PROJECT_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY), is(TEST_DATASET_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY), is(TEST_TABLE_ID));
    assertThat(conf.get(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY), is(TEST_FILE_FORMAT.name()));
    assertThat(
        conf.get(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY), is(TEST_OUTPUT_CLASS.getName()));
    Truth.assertThat(conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY)).isNull();
    assertThat(
        BigQueryOutputConfiguration.getGcsOutputPath(conf).toString(), is(TEST_OUTPUT_PATH_STRING));
  }

  /** Test an exception is thrown if a required parameter is missing for configure. */
  @Test
  public void testConfigureMissing() throws IOException {
    // Missing the PROJECT_ID.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BigQueryOutputConfiguration.configure(
                conf,
                QUALIFIED_TABLE_ID_WITHOUT_PROJECT,
                TEST_TABLE_SCHEMA,
                TEST_OUTPUT_PATH_STRING,
                TEST_FILE_FORMAT,
                TEST_OUTPUT_CLASS));
  }

  /** Test the validateConfiguration function doesn't error on expected input. */
  @Test
  public void testValidateConfiguration() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);

    BigQueryOutputConfiguration.validateConfiguration(conf);
  }

  /** Test the validateConfiguration errors on missing project id. */
  @Test
  public void testValidateConfigurationMissingProjectId() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);
    conf.unset(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.validateConfiguration(conf));
  }

  /** Test the validateConfiguration errors on missing table schema. */
  @Test
  public void testValidateConfigurationBadSchema() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, TEST_BAD_TABLE_SCHEMA_STRING);

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.validateConfiguration(conf));
  }

  /** Test the validateConfiguration errors on missing file format. */
  @Test
  public void testValidateConfigurationMissingFileFormat() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);
    conf.unset(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY);

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.validateConfiguration(conf));
  }

  /** Test the validateConfiguration errors on missing output format class. */
  @Test
  public void testValidateConfigurationMissingOutputFormat() throws IOException {
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);
    conf.unset(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY);

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.validateConfiguration(conf));
  }

  /** Test the getProjectId returns the correct data. */
  @Test
  public void testGetProjectId() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, TEST_PROJECT_ID);

    String result = BigQueryOutputConfiguration.getProjectId(conf);

    assertThat(result, is(TEST_PROJECT_ID));
  }

  /** Test the getProjectId returns the correct data. */
  @Test
  public void testGetProjectIdBackup() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, TEST_PROJECT_ID);

    String result = BigQueryOutputConfiguration.getProjectId(conf);

    assertThat(result, is(TEST_PROJECT_ID));
  }

  /** Test the getProjectId errors on missing data. */
  @Test
  public void testGetProjectIdMissing() throws IOException {
    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.getProjectId(conf));
  }

  /** Test the getTable returns the correct data. */
  @Test
  public void testGetTableReference() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, TEST_PROJECT_ID);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, TEST_DATASET_ID);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, TEST_TABLE_ID);

    TableReference result = BigQueryOutputConfiguration.getTableReference(conf);

    assertThat(result, is(TEST_TABLE_REF));
  }

  /** Test the getTable throws an exception when a key is missing. */
  @Test
  public void testGetTableReferenceMissingKey() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, TEST_PROJECT_ID);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, TEST_DATASET_ID);
    // Missing TABLE_ID

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.getTableReference(conf));
  }

  /** Test the getTable does not throw an exception when a backup project id is found. */
  @Test
  public void testGetTableReferenceBackupProjectId() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, TEST_PROJECT_ID);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, TEST_DATASET_ID);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, TEST_PROJECT_ID);

    BigQueryOutputConfiguration.getTableReference(conf);
  }

  /** Test the getTableSchema returns the correct data. */
  @Test
  public void testGetTableReferenceSchema() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, TEST_TABLE_SCHEMA_STRING);

    Optional<BigQueryTableSchema> result = BigQueryOutputConfiguration.getTableSchema(conf);

    assertThat(result.get(), is(TEST_TABLE_SCHEMA));
  }

  /** Test the getTableSchema throws an exception when the schema is malformed. */
  @Test
  public void testGetTableReferenceSchemaBadSchema() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, TEST_BAD_TABLE_SCHEMA_STRING);

    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.getTableSchema(conf));
  }

  /** Test the getFileFormat returns the correct data. */
  @Test
  public void testGetFileFormat() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY, TEST_FILE_FORMAT.name());

    BigQueryFileFormat result = BigQueryOutputConfiguration.getFileFormat(conf);

    assertThat(result, is(TEST_FILE_FORMAT));
  }

  /** Test the getFileFormat errors when it's missing. */
  @Test
  public void testGetFileFormatMissing() throws IOException {
    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.getFileFormat(conf));
  }

  /** Test the getFileFormat errors when it's malformed. */
  @Test
  public void testGetFileFormatMalformed() throws IOException {
    conf.set(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY, TEST_FILE_FORMAT.name().toLowerCase());

    assertThrows(
        IllegalArgumentException.class, () -> BigQueryOutputConfiguration.getFileFormat(conf));
  }

  /** Test the getGcsOutputPath returns the correct data. */
  @Test
  public void testGetGcsOutputPath() throws IOException {
    BigQueryOutputConfiguration.setFileOutputFormatOutputPath(conf, TEST_OUTPUT_PATH_STRING);

    Path result = BigQueryOutputConfiguration.getGcsOutputPath(conf);

    assertThat(result.toString(), is(TEST_OUTPUT_PATH_STRING));
  }

  /** Test the getGcsOutputPath errors when it's missing. */
  @Test
  public void testGetGcsOutputPathMissing() throws IOException {
    assertThrows(IOException.class, () -> BigQueryOutputConfiguration.getGcsOutputPath(conf));
  }
}
