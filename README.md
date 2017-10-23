# bigdata-interop

Libraries and tools for interoperability between Hadoop-related open-source software and Google Cloud Platform.

## Google Cloud Storage connector for Hadoop

The Google Cloud Storage connector for Hadoop enables running MapReduce jobs
directly on data in Google Cloud Storage by implementing the Hadoop FileSystem
interface. For details, see the README in the `/gcs/` folder.

### Building

The Google Cloud Storage (GCS) connector is built with Maven 3 (as of 2014-05-23, version 3.2.1 has been tested). To build the connector for Hadoop 1, run the following commands from the main directory:

    mvn -P hadoop1 package

To build the connector with support for Hadoop 2 & YARN, run the following commands from the main directory:

    mvn -P hadoop2 package

In both cases the GCS connector JAR can be found in gcs/target/.
