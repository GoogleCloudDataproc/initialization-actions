# Google Cloud Storage Connector for Spark and Hadoop

The Google Cloud Storage connector for Hadoop lets you run
[Apache Hadoop](http://hadoop.apache.org) or
[Apache Spark](http://spark.apache.org) jobs directly on data in
[Google Cloud Storage](https://cloud.google.com/storage), and offers a number of
benefits over choosing Hadoop Distributed File System (HDFS) as your default
file system.

## Benefits of using the connector

Choosing Cloud Storage alongside the
[Hadoop Distributed File System (HDFS)](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
has several benefits:

*   **Direct data access** - Store your data in Cloud Storage and access it
    directly, with no need to transfer it into HDFS first.
*   **HDFS compatibility** - You can store data in HDFS in addition to Cloud
    Storage, and access it with the connector by using a different file path.
*   **Interoperability** - Storing data in Cloud Storage enables seamless
    interoperability between Spark, Hadoop, and other Google services.
*   **Data accessibility** - When you shut down a Hadoop cluster, you still have
    access to your data in Cloud Storage, unlike HDFS.
*   **High data availability** - Data stored in Cloud Storage is highly
    available and globally replicated without a performance hit.
*   **No storage management overhead** - Unlike HDFS, Cloud Storage requires no
    routine maintenance such as checking the file system, upgrading or rolling
    back to a previous version of the file system, etc.
*   **Quick startup** - In HDFS, a MapReduce job can't start until the NameNode
    is out of safe mode—a process that can take from a few seconds to many
    minutes depending on the size and state of your data. With Google Cloud
    Storage, you can start your job as soon as the task nodes start, leading to
    significant cost savings over time.

## Getting the connector

This repository contains the Hadoop 2.x and the Hadoop 3.x compatible connector.
You can clone this repository and follow the directions in `INSTALL.md` within
this directory to install the connector. If you use
[Google Cloud Dataproc](https://cloud.google.com/dataproc) the connector is
installed automatically.

## Configuring the connector

When you set up a Hadoop cluster by following the directions in `INSTALL.md`,
the cluster is automatically configured for optimal use with the connector.
Typically, there is no need for further configuration.

To customize the connector, specify configuration values in `core-site.xml` in
the Hadoop configuration directory on the machine on which the connector is
installed.

For a complete list of configuration keys and their default values see
[CONFIGURATION.md](/gcs/CONFIGURATION.md).

## Accessing Cloud Storage data

There are multiple ways to access data stored in Google Cloud Storage:

*   The hadoop shell: `hadoop fs -ls gs://<CONFIGBUCKET>/dir/file` (recommended)
    or `fs -ls /dir/file`
*   the
    [Cloud Platform Console Cloud Storage browser](https://cloud.google.com/storage/docs/gettingstarted-console)
*   [`gsutil cp`](https://cloud.google.com/storage/docs/gsutil/commands/cp)
*   [`gsutil rsync`](https://cloud.google.com/storage/docs/gsutil/commands/rsync)
*   The
    [Cloud Storage JSON API](https://cloud.google.com/storage/docs/json_api/v1/)
