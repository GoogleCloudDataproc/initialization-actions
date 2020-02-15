### 1.1.0 - 2020-XX-XX


### 1.0.1 - 2020-02-13

1.  POM updates for GCS connector 2.1.0.

1.  Fix shaded jar - add back missing relocated dependencies.

### 1.0.0 - 2019-08-23

1.  POM updates for GCS connector 2.0.0.

1.  Remove Hadoop 1.x support.

1.  Remove deprecated features and associated properties:

        mapred.bq.input.query
        mapred.bq.query.results.table.delete
        mapred.bq.input.sharded.export.enable

1.  Remove obsolete `mapred.bq.output.async.write.enabled` property.

1.  Support nested record type in field schema in BigQuery connector.

1.  Remove dependency on GCS connector code.

1.  Add a property to specify BigQuery tables partitioning definition:

        mapred.bq.output.table.partitioning

1.  Add a new `DirectBigQueryInputFormat` for processing data through
    [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/).

    This input format is configurable via properties:

        mapred.bq.input.sql.filter
        mapred.bq.input.selected.fields
        mapred.bq.input.skew.limit

1.  Update all dependencies to latest versions.

1.  Add a property to control max number of attempts when polling for next file.
    By default max number of attempts is unlimited (`-1` value):

        mapred.bq.dynamic.file.list.record.reader.poll.max.attempts (default: -1)

1.  Add a property to specify output table create disposition:

        mapred.bq.output.table.createdisposition (default: CREATE_IF_NEEDED)

### 0.13.14 - 2019-02-13

1.  POM updates for GCS connector 1.9.14.

### 0.13.13 - 2019-02-04

1.  POM updates for GCS connector 1.9.13.

### 0.13.12 - 2019-01-30

1.  POM updates for GCS connector 1.9.12.

1.  Improve exception message for BigQuery job execution errors.

1.  Update all dependencies to latest versions.

### 0.13.11 - 2018-12-20

1.  POM updates for GCS connector 1.9.11.

1.  Update all dependencies to latest versions.

### 0.13.10 - 2018-11-01

1.  POM updates for GCS connector 1.9.10.

### 0.13.9 - 2018-10-19

1.  POM updates for GCS connector 1.9.9.

### 0.13.8 - 2018-10-03

1.  POM updates for GCS connector 1.9.8.

### 0.13.7 - 2018-09-20

1.  POM updates for GCS connector 1.9.7.

### 0.13.6 - 2018-08-30

1.  POM updates for GCS connector 1.9.6.

1.  Migrate logging to Google Flogger.

    To configure Log4j as a Flogger backend set `flogger.backend_factory` system
    property to
    `com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance` or
    `com.google.cloud.hadoop.repackaged.bigquery.com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance`
    if using shaded jar.

    For example:

        java -Dflogger.backend_factory=com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance ...

1.  Poll BQ jobs in their correct locations.

1.  Update all dependencies to latest versions.

### 0.13.5 - 2018-08-09

1.  POM updates for GCS connector 1.9.5.

1.  Improve build configuration (`pom.xml`s) compatibility with Maven release
    plugin.

    Changes version string from `0.13.5-hadoop2` to `hadoop2-0.13.5`.

1.  Update Maven plugins versions.

### 0.13.4 - 2018-08-07

1.  POM updates for GCS connector 1.9.4.

1.  Update all dependencies to latest versions.

### 0.13.3 - 2018-07-25

1.  POM updates for GCS connector 1.9.3.

1.  Fix Ivy compatibility - resolve artifact versions in released `pom.xml`
    files.

### 0.13.2 - 2018-07-18

1.  POM updates for GCS connector 1.9.2.

### 0.13.1 - 2018-07-11

1.  POM updates for GCS connector 1.9.1.

### 0.13.0 - 2018-06-15

1.  POM updates for GCS connector 1.9.0.

1.  Update all dependencies to latest versions.

1.  Change Maven project structure to be better compatible with IDEs.

1.  Support Hadoop 3.

1.  Default `BigQueryInputFormats` to use unsharded exports and deprecate
    sharded exports.

1.  Deprecate `BigQueryOutputFormat` in favor of `IndirectBigQueryOutputFormat`.

1.  Add interface through which user can directly provide the access token.

1.  Support Cloud KMS key name in the output table spec.

### 0.12.1 - 2018-03-29

1.  Wire location through load, extract, and query jobs.

1.  Always require at least 2 partitions for sharded exports.

1.  POM updates for GCS connector 1.8.1.

### 0.12.0 - 2018-03-15

1.  POM updates for GCS connector 1.8.0.

1.  Change relocation package in shaded jar to be connector-specific.

1.  Min required Java version now is Java 8.

### 0.11.0 - 2018-02-22

1.  Relocate all dependencies in shaded jar.

1.  Update all dependencies to latest versions.

1.  POM updates for GCS connector 1.7.0.

### 0.10.3 - 2017-11-21

1.  POM updates for GCS connector 1.6.2.

### 0.10.2 - 2017-04-14

1.  POM updates for GCS connector 1.6.1.

### 0.10.1 - 2016-12-16

1.  Added a configurable write disposition when using
    `IndirectBigQueryOutputFormat` with `WRITE_APPEND` as the default.

### 0.10.0 - 2016-11-07

1.  Update output configuration keys to conform to the format in
    `BigQueryConfiguration` and have `BigQueryOutputConfiguration` handle the
    output path resolution and configuration.

### 0.9.0 - 2016-11-04

1.  Added temporary-path resolution to `BigQueryConfiguration` to automatically
    default to the GCS-specified system bucket if the BigQuery specific GCS
    bucket isn't specified and an explicit temporary GCS path for BigQuery isn't
    specified.

1.  Refactored the new `IndirectBigQueryOutputFormat` class hierarchy to require
    less boilerplate in format-specific implementations.

1.  Added support for federated BigQuery table outputs using the
    `IndirectBigQueryOutputFormat`.

### 0.8.1 - 2016-10-10

1.  Misc minor refactoring.

### 0.8.0 - 2016-10-05

1.  Added new still-experimental versions of BigQuery output which buffer to GCS
    before issuing the BigQuery load jobs, rather than using temporary BigQuery
    tables.

1.  Updated Hadoop 2 dependency to 2.7.2.

### 0.7.9 - 2016-09-21

1.  Upgraded BigQuery library dependency to rev319.

1.  Added support for reading pre-existing federated Avro and JSON tables stored
    in Google Cloud Storage.

1.  Misc updates in credential acquisition on GCE.

### 0.7.8 - 2016-08-23

1.  Updated `AbstractGoogleAsyncWriteChannel` to always set the
    `X-Goog-Upload-Desired-Chunk-Granularity` header independently from the
    deprecated `X-Goog-Upload-Max-Raw-Size`; in general this improves
    performance of large uploads.

### 0.7.7 - 2016-07-29

1.  Misc updates in GCS-related library dependencies.

### 0.7.6 - 2016-07-15

1.  Changed `RetryHttpInitializer` to treat HTTP code 429 as a retriable error
    with exponential backoff instead of erroring out.

1.  Misc updates in GCS-related library dependencies.

1.  Added location support for temporary dataset, configured with the key
    `mapred.bq.output.location`. Default is `US`.

### 0.7.5 - 2016-03-24

1.  Misc updates in GCS-related library dependencies.

### 0.7.4 - 2016-02-02

1.  Fixes handling of sharded Avro splits where Avro files contain more than a
    single file split.

### 0.7.3 - 2015-11-12

1.  Fixes handling of 'unsharded' BigQuery exports.

1.  Shades Apache HTTP components that conflict with Hadoop dependencies.

1.  Misc updates in error detection logic.

### 0.7.2 - 2015-09-15

1.  Misc updates in GCS-related library dependencies.

### 0.7.1 - 2015-07-09

1.  Misc updates in library dependencies; google.api.version
    (com.google.http-client, com.google.api-client) updated from 1.19.0 to
    1.20.0, google-api-services-storage from v1-rev16-1.19.0 to v1-rev35-1.20.0,
    and google-api-services-bigquery from v2-rev171-1.19.0 to v2-rev217-1.20.0,
    and Guava from 17.0 to 18.0.

### 0.7.0 - 2015-05-27

1.  All logging converted to use slf4j instead of the previous
    `org.apache.commons.logging.Log`; removed the `LogUtil` wrapper which
    previously wrapped `org.apache.commons.logging.Log`.

1.  Added exponential-backoff automatic retries in `waitForJobCompletion`.

1.  Added support for running multiple concurrent jobs writing to the same
    output dataset by including the `JobID` as part of the temporary
    `datasetId`.

### 0.6.0 - 2015-02-26

1.  Fixed a bug in BigQueryOutputFormat which can occasionally cause extraneous
    records to be written due to low-level retries not be de-duplicated on the
    server side; temporary tables are now written with `WRITE_TRUNCATE` instead
    of `WRITE_APPEND` to get around the lack of de-duplication of low-level
    retries.

1.  Removed the `BigQueryBatchedWriteChannel`, which was previously the default
    behavior and controlled by `mapred.bq.output.async.write.enabled` defaulting
    to `false`; default is now `true` and the key is ignored; the batched
    channel is fundamentally vulnerable to low-level retries causing extraneous
    records to be written. `BigQueryAsyncWriteChannel` is now always used, which
    improves performance and reduces the number of "load" jobs over the course
    of a mapreduce; it is now equal to the number of reduce tasks, rather than
    being equal to the total `output_size` divided by the `batch_size`.

1.  All BigQuery job insertions now supply client-generated job_ids and handle
    409 conflict as a duplicate job caused by low-level retries.

### 0.5.1 - 2015-01-22

1.  Added enforcement of maximum number of export shards (currently 500) when
    calculating splits for `BigQueryIntputFormat`.

1.  Fixed a bug where `BigQueryOutputCommitter.needsTaskCommit()` incorrectly
    depended on a `Bigquery.Tables.list()` call; listing tables suffers
    "eventual consistency", so occasionally a task would erroneously fail to
    commit data.

1.  Removed extraneous table-deletion in `BigQueryOutputCommitter.abortTask()`;
    cleanup occurs during job cleanup anyways, and this would incorrectly (but
    harmlessly) try to delete a nonexistent table for map tasks.

### 0.5.0 - 2014-12-16

1.  `BigQueryInputFormat` has been renamed `GsonBigQueryInputFormat` to better
    reflect its nature as a GSON-based format. A forwarding declaration was left
    in place to maintain compatibility.

1.  `JsonTextBigQueryInputFormat` was added to provide lines of JSON text as
    they appear in the BigQuery export.

1.  When using sharded BigQuery exports (the default), the keys will no longer
    be in increasing order per mapper. Instead, the keys will be as they are
    reported by the delegate `RecordReader` which is generally going to be the
    byte position within the current file. However, the sharded export creates
    many files per mapper so this position will appear to reset to 0 when we
    switch between files. The record reader's getProgress() will still report
    progress across the entire dataset that the record reader is responsible
    for.

1.  The BigQuery connector can now ingest Avro based BigQuery exports. Using and
    Avro-based export should result in less data transferred between your
    MapReduce job and Google Cloud Storage and should require less CPU time to
    parse the data files. To use Avro, set the input format to
    `AvroBigQueryInputFormat` and update your map code to expect `LongWritable`
    keys and Avro `GenericData.Record` values.

1.  Hadoop 2 support was added for Java MapReduce. Streaming support for Hadoop
    2 will be included in a future release.

### 0.4.5 - 2014-10-17

1.  Attempting to acquire an OAuth access token will be now be retried when
    using `.p12` or installed application (JWT) credentials if there is a
    recoverable error such as an HTTP 5XX response code or an IOException.

### 0.4.4 - 2014-09-18

1.  Added new classes implementing the `hadoop.mapred.*` interfaces by wrapping
    the existing `hadoop.mapreduce.*` implementations and delegating
    appropriately. This enables backwards-compatibility for some stacks which
    depend on the "old api" interfaces, including now being able to use the
    standard `hadoop-streaming.jar` to run binary mappers/reducers with the
    BigQuery connector. Note that in the absence of a blocking driver program to
    call `BigQueryInputFormat.cleanupJob`, you must instead explicitly clean up
    the temporary exported files after a hadoop-streaming job if using the input
    connector. Extra cleanup is not necessary if only using the output connector
    in hadoop-streaming. The new top-level classes:

        com.google.cloud.hadoop.io.bigquery.mapred.BigQueryMapredInputFormat
        com.google.cloud.hadoop.io.bigquery.mapred.BigQueryMapredOutputFormat

    See the javadocs for the associated `RecordReader/Writer`, `InputSplit`, and
    `OutputCommitter` classes.

### 0.4.3 - 2014-08-07

1.  Added better validation to `BigQueryUtils.getSchemaFromString` used by
    `BigQueryOutputFormat` to throw descriptive `IllegalArgumentExceptions`
    instead of `NullPointerExceptions` for most types of malformed schemas.

1.  Fixed a bug in `BigQueryUtils.getSchemaFromString` to support 'repeated'
    fields inside of nested records; used to throw `IllegalStateException` if a
    nested record contained more than 1 inner field.

### 0.4.2 - 2014-06-05

1.  Misc updates in library dependencies.

### 0.4.1 - 2014-05-08

1.  Removed `mapred.bq.output.num.records.batch` in favor of
    `mapred.bq.output.buffer.size`.

1.  Misc updates in library dependencies.

### 0.4.0 - 2014-04-09

1.  Preview release of BigQuery connector.

1.  Added support for different projectIds owning the input/output tables in
    BigQuery vs the `projectId` performing the BigQuery jobs. Necessary for
    reading public tables like `publicdata:samples.shakespeare`. Distinguished
    by `mapred.bq.input.project.id` and `mapred.bq.output.project.id`.

1.  Deprecated `mapred.bq.input.query` and modified the WordCount sample to
    eliminate usage of the query.

1.  Added a new `BigQueryOutputFormat` mode which uses resumable uploads; can be
    enabled with `mapred.bq.output.async.write.enabled`.

1.  Jar file renamed to `bigquery-connector-<version>jar`.

### 0.3.0 - 2014-03-21

1.  Added `CHANGES.txt` for release notes to be included in connector jarfile.

1.  Fixed a bug where different task attempts could collide (either through
    retries or speculative execution) by using full `TaskAttemptID` for temp
    tableIds in `BigQueryOutputFormat`.

1.  Expanded debug logging, especially in the OutputFormat and helpers.

1.  Modified `BigQueryOutputCommitter` to correctly use the CopyTable API and
    avoid incurring "query" costs in the output path.

1.  Eliminated the need to call `BigQueryInputFormat.setInputs(Job)` in main
    class; this is now automatically set if necessary inside `getSplits`.

1.  The field `mapred.bq.temp.gcs.path` is now optional; auto-generated based on
    `JobID` in `BigQueryInputFormat.getSplits` from `mapred.bq.gcs.bucket` if
    unspecified. No longer set in
    `BigQueryConfiguration.configureBigQueryInput`.

1.  Fixed `BigQueryInputFormat` to only delete the input BigQuery table if a
    query was actually run AND `mapred.bq.query.results.table.delete` is true;
    changed the latter's default value from `true` to `false`.

1.  Fixed a bug where JsonRecordReader.initialize was getting called twice,
    which resulted in extraneous GCS API calls.

1.  Implemented a new "sharded" export mode for the `BigQueryInputFormat` which
    allows the MapReduce to progress concurrently with the BigQuery export;
    `RecordReaders` will read files as they become available, or block if more
    files are expected but are not yet available. Toggled on/off using
    `mapred.bq.input.sharded.export.enable = [true|false]`. The number of export
    directories is based on `mapred.map.tasks`, but may automatically choose a
    smaller number of shards if the BigQuery table is small. This mode is now
    the default export mode for `BigQueryInputFormat`.

1.  Changed credential configuration values to allow per connector overrides. To
    use the metadata service, no extra configuration is required. To a use
    PKCS12 private key file, specify `mapred.bq.auth.service.account.email` and
    `mapred.bq.auth.service.account.keyfile`. To use the installed app workflow,
    set `mapred.bq.service.account.enable` to `false` and
    `mapred.bq.auth.client.id`, `mapred.bq.auth.client.secret` and
    `mapred.bq.auth.client.file` to appropriate values. Both PKCS12 and
    installed app workflows require the credential file to exist on all nodes
    and at the same path.

1.  Removed cleanup of things related to the `BigQueryInputFormat` from the
    `BigQueryOutputCommitter` so that Input/Output formats operate
    independently. Now, the main class must call
    `BigQueryInputFormat.cleanupJob(JobContext)` at the end of a job explicitly.

### 0.2.0 - 2014-02-12

1.  Compiled connector examples and scripts for running the sample mapreduce now
    available in `bq-toolkit-0.2.0.tar.gz`.

1.  Added low-level retries for transient server errors.

1.  Improved debug logging.

1.  Fixed a bug where the connector failed to perform the BigQuery export if
    `DEFAULT_FS` was set to `hdfs`.

1.  GCS export paths no longer contain the `jobIdentifier`, and instead are
    written to `gs://<bucket>/hadoop/tmp/bigquery/data-<datetime
    string>/data-*.json`.
