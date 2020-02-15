# bigdata-interop

[![GitHub release](https://img.shields.io/github/release/GoogleCloudDataproc/bigdata-interop.svg)](https://github.com/GoogleCloudDataproc/bigdata-interop/releases/latest)
[![GitHub release date](https://img.shields.io/github/release-date/GoogleCloudDataproc/bigdata-interop.svg)](https://github.com/GoogleCloudDataproc/bigdata-interop/releases/latest)
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/GoogleCloudDataproc/bigdata-interop.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/GoogleCloudDataproc/bigdata-interop/context:java)
[![codecov](https://codecov.io/gh/GoogleCloudDataproc/bigdata-interop/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudDataproc/bigdata-interop)

Libraries and tools for interoperability between Apache Hadoop related
open-source software and Google Cloud Platform.

## Google Cloud Storage connector for Apache Hadoop

[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector/hadoop1.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:gcs-connector%20AND%20v:hadoop1-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector/hadoop2.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:gcs-connector%20AND%20v:hadoop2-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/gcs-connector/hadoop3.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:gcs-connector%20AND%20v:hadoop3-*)

The Google Cloud Storage connector for Hadoop enables running MapReduce jobs
directly on data in Google Cloud Storage by implementing the Hadoop FileSystem
interface. For details, see [the README](gcs/README.md).

## Google BigQuery connector for Apache Hadoop MapReduce

[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/bigquery-connector/hadoop1.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:bigquery-connector%20AND%20v:hadoop1-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/bigquery-connector/hadoop2.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:bigquery-connector%20AND%20v:hadoop2-*)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.bigdataoss/bigquery-connector/hadoop3.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.google.cloud.bigdataoss%20AND%20a:bigquery-connector%20AND%20v:hadoop3-*)

The Google BigQuery connector for Hadoop MapReduce enables running MapReduce
jobs on data in BigQuery by implementing the InputFormat & OutputFormat
interfaces. For more details see
[the documentation](https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery)

## Google Cloud Pub/Sub connector for Apache Spark Streaming

This connector is deprecated and was removed - it's recommended to use
[Apache Bahir](https://bahir.apache.org/) instead.

## Building the Cloud Storage and BigQuery connectors

To build the connector for specific Hadoop version, run the following commands
from the main directory:

```bash
# with Hadoop 2 and YARN support:
./mvnw -P hadoop2 clean package
# with Hadoop 3 and YARN support:
./mvnw -P hadoop3 clean package
```

In order to verify test coverage for specific Hadoop version, run the following
commands from the main directory:

```bash
# with Hadoop 2 and YARN support:
./mvnw -P hadoop2 -P coverage clean verify
# with Hadoop 3 and YARN support:
./mvnw -P hadoop3 -P coverage clean verify
```

The Cloud Storage connector JAR can be found in `gcs/target/`. The BigQuery
connector JAR can be found in `bigquery/target/`.

## Adding the Cloud Storage and BigQuery connectors to your build

Maven group ID is `com.google.cloud.bigdataoss` and artifact ID for Cloud
Storage connector is `gcs-connector` and for BigQuery connectors is
`bigquery-connector`.

To add a dependency on one of the connectors using Maven, use the following:

```xml
<dependency>
  <groupId>com.google.cloud.bigdataoss</groupId>
  <!-- Cloud Storage: -->
  <artifactId>gcs-connector</artifactId>
  <version>hadoop2-2.0.1</version>
  <!-- or, for BigQuery: -->
  <artifactId>bigquery-connector</artifactId>
  <version>hadoop2-1.0.1</version>
</dependency>
```

## Resources

On **Stack Overflow**, use the tag
[`google-cloud-dataproc`](https://stackoverflow.com/tags/google-cloud-dataproc)
for questions about the connectors in this repository. This tag receives
responses from the Stack Overflow community and Google engineers, who monitor
the tag and offer unofficial support.
