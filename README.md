# bigdata-interop

Libraries and tools for interoperability between Apache Hadoop related
open-source software and Google Cloud Platform.

## Google Cloud Storage connector for Apache Hadoop

The Google Cloud Storage connector for Hadoop enables running MapReduce jobs
directly on data in Google Cloud Storage by implementing the Hadoop FileSystem
interface. For details, see [the README](gcs/README.md).

## Google BigQuery connector for Apache Hadoop MapReduce

The Google BigQuery connector for Hadoop MapReduce enables running MapReduce
jobs on data in BigQuery by implementing the InputFormat & OutputFormat
interfaces. For more details see
[the documentation](https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery)

## Google Cloud Pub/Sub connector for Apache Spark Streaming

The Google Cloud Pub/Sub connector for Spark Streaming enables running Spark
Streaming Job on topics in Pub/Sub by implementing the InputDStream interface.
For more details see [the README](pubsub/README.md)

## Building the Cloud Storage (GCS) and BigQuery connectors

All the connectors can be built with Apache Maven 3 (as of 2018-08-07, version
3.5.4 has been tested). To build the connector for specific Hadoop version, run
the following commands from the main directory:

```bash
# with Hadoop 1 support:
mvn -P hadoop1 clean package
# with Hadoop 2 and YARN support:
mvn -P hadoop2 clean package
# with Hadoop 3 and YARN support:
mvn -P hadoop3 clean package
```

The GCS connector JAR can be found in `gcs/target/`. The BigQuery JAR can be
found in `bigquery/target/`.

### Building the Google Cloud Pub/Sub connector

Building the Pub/Sub Connector is described in
[its README](pubsub/README.md#building-and-testing).

## Adding the Cloud Storage (GCS) and BigQuery connectors to your build

Maven group ID is `com.google.cloud.bigdataoss` and artifact ID for Cloud
Storage connector is `gcs-connector` and for BigQuery connectors is
`bigquery-connector`.

To add a dependency on one of the connectors using Maven, use the following:

```xml
<dependency>
  <groupId>com.google.cloud.bigdataoss</groupId>
  <!-- Cloud Storage: -->
  <artifactId>gcs-connector</artifactId>
  <version>hadoop2-1.9.6</version>
  <!-- or, for BigQuery: -->
  <artifactId>bigquery-connector</artifactId>
  <version>hadoop2-0.13.6</version>
</dependency>
```
