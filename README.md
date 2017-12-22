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
interfaces. For more details see [the documentation](
https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery)

## Google Cloud Pub/Sub connector for Apache Spark Streaming

The Google Cloud Pub/Sub connector for Spark Streming enables running Spark
Streaming Job on topics in Pub/Sub by implementing the InputDStream
interface. For more details see [the README](pubsub/README.md)

## Building

All the connectors can be built with Apache Maven 3 (as of 2017-10-25,
version 3.5.0 has been tested). To build the connector for Hadoop 1,
run the following commands from the main directory:

    mvn -P hadoop1 package

To build the connector with support for Hadoop 2 & YARN, run the following
commands from the main directory:

    mvn -P hadoop2 package

The GCS connector JAR can be found in gcs/target/. The BigQuery JAR can be
found in bigquery/target.

### Building the Google Cloud Pub/Sub connector

Building the Pub/Sub Connector is described in [its
README](pubsbu/README.md#building-and-testing).
