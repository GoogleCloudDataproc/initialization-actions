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
package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions.Builder;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.DataFormat;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat that directly reads data from BigQuery using the BigQuery Storage API. See
 * https://cloud.google.com/bigquery/docs/reference/storage/.
 */
@InterfaceStability.Evolving
public class DirectBigQueryInputFormat extends InputFormat<NullWritable, GenericRecord> {

  private static final String DIRECT_PARALLELISM_KEY = MRJobConfig.NUM_MAPS;
  private static final int DIRECT_PARALLELISM_DEFAULT = 10;

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration configuration = context.getConfiguration();
    BigQueryStorageClient client = getClient(configuration);
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }
    double skewLimit =
        configuration.getDouble(
            BigQueryConfiguration.SKEW_LIMIT_KEY, BigQueryConfiguration.SKEW_LIMIT_DEFAULT);
    Preconditions.checkArgument(
        skewLimit >= 1.0,
        "%s is less than 1; not all records would be read. Exiting",
        BigQueryConfiguration.SKEW_LIMIT_KEY);
    Table table = getTable(configuration, bigQueryHelper);
    ReadSession session = startSession(configuration, table, client);
    long numRows = table.getNumRows().longValue();
    long limit = Math.round(skewLimit * numRows / session.getStreamsCount());

    return session.getStreamsList().stream()
        .map(
            stream ->
                new DirectBigQueryInputSplit(
                    stream.getName(), session.getAvroSchema().getSchema(), limit))
        .collect(Collectors.toList());
  }

  private static Table getTable(Configuration configuration, BigQueryHelper bigQueryHelper)
      throws IOException {
    Map<String, String> mandatoryConfig =
        ConfigurationUtil.getMandatoryConfig(
            configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    String inputProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY);

    TableReference tableReference =
        new TableReference()
            .setDatasetId(datasetId)
            .setProjectId(inputProjectId)
            .setTableId(tableName);
    return bigQueryHelper.getTable(tableReference);
  }

  private static ReadSession startSession(
      Configuration configuration, Table table, BigQueryStorageClient client) {
    // Extract relevant configuration settings.
    String jobProjectId = configuration.get(BigQueryConfiguration.PROJECT_ID_KEY);
    String filter = configuration.get(BigQueryConfiguration.SQL_FILTER_KEY, "");
    Collection<String> selectedFields =
        configuration.getStringCollection(BigQueryConfiguration.SELECTED_FIELDS_KEY);

    Builder readOptions = TableReadOptions.newBuilder().setRowRestriction(filter);
    if (!selectedFields.isEmpty()) {
      readOptions.addAllSelectedFields(selectedFields);
    }
    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder()
            .setTableReference(
                TableReferenceProto.TableReference.newBuilder()
                    .setProjectId(table.getTableReference().getProjectId())
                    .setDatasetId(table.getTableReference().getDatasetId())
                    .setTableId(table.getTableReference().getTableId()))
            .setRequestedStreams(getParallelism(configuration))
            .setParent("projects/" + jobProjectId)
            .setReadOptions(readOptions)
            .setFormat(DataFormat.AVRO)
            .build();
    return client.createReadSession(request);
  }

  private static int getParallelism(Configuration configuration) {
    return configuration.getInt(DIRECT_PARALLELISM_KEY, DIRECT_PARALLELISM_DEFAULT);
  }

  @Override
  public RecordReader<NullWritable, GenericRecord> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext context) {
    return new DirectBigQueryRecordReader();
  }

  /**
   * Helper method to override for testing.
   *
   * @return Bigquery.
   * @throws IOException on IO Error.
   */
  protected BigQueryStorageClient getClient(Configuration config) throws IOException {
    return BigQueryStorageClient.create();
  }

  /** Helper method to override for testing. */
  protected BigQueryHelper getBigQueryHelper(Configuration config)
      throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactory();
    return factory.getBigQueryHelper(config);
  }

  /** InputSplit containing session metadata. */
  public static class DirectBigQueryInputSplit extends InputSplit implements Writable {

    private String name;
    private String schema;
    private long limit;

    public DirectBigQueryInputSplit() {}

    public DirectBigQueryInputSplit(String name, String schema, long limit) {
      this.name = name;
      this.schema = schema;
      this.limit = limit;
    }

    @Override
    public long getLength() {
      // Unknown because of dynamic allocation
      return -1;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(name);
      out.writeUTF(schema);
      out.writeLong(limit);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      name = in.readUTF();
      schema = in.readUTF();
      limit = in.readLong();
    }

    public String getName() {
      return name;
    }

    public String getSchema() {
      return schema;
    }

    public long getLimit() {
      return limit;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, schema, limit);
    }

    private Object[] values() {
      return new Object[] {name, schema, limit};
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof DirectBigQueryInputSplit)) {
        return false;
      }
      return Arrays.equals(values(), ((DirectBigQueryInputSplit) o).values());
    }

    @Override
    public String toString() {
      return String.format("(name='%s', schema='%s', limit='%s')", name, schema, limit);
    }
  }
}
