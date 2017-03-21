# Installing the connector

To install the connector manually, complete the following steps.

## Ensure authenticated Cloud Storage access

Depending on where the machines which comprise your cluster are located, you must do one of the following:

* **Google Cloud Platform** - Each Google Compute Engine VM must be [configured to have access](https://cloud.google.com/compute/docs/authentication#using) to the [Cloud Storage scope](https://cloud.google.com/storage/docs/authentication#oauth) you intend to use the connector for.
* **non-Google Cloud Platform** - Obtain an [OAuth 2.0 private key](https://cloud.google.com/storage/docs/authentication#generating-a-private-key). Installing the connector on a machine other than a GCE VM can lead to higher Cloud Storage access costs. For more information, see [Cloud Storage Pricing](https://cloud.google.com/storage/pricing).

## Add the connector jar to Hadoop's classpath

Placing the connector jar in the appropriate subdirectory of the Hadoop installation may be effective to have Hadoop load the jar. However, to be certain that the jar is loaded, add `HADOOP_CLASSPATH=$HADOOP_CLASSPATH:</path/to/gcs-connector-jar>` to `hadoop-env.sh` in the Hadoop configuration directory.

## Configure Hadoop

Based on the steps in configuring the connector, you must add the following two properties to `core-site.xml`.

    <property>
      <name>fs.gs.impl</name>
      <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
      <description>The FileSystem for gs: (GCS) uris.</description>
    </property>
    <property>
      <name>fs.AbstractFileSystem.gs.impl</name>
      <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
      <description>
        The AbstractFileSystem for gs: (GCS) uris. Only necessary for use with Hadoop 2.
      </description>
    </property>

If you chose to use a private key for Cloud Storage Authorizaton, make sure to set the necessary `google.cloud.auth` values documented in [gcs-core-default.xml](/conf/gcs-core-default.xml) inside the `conf` directory.

## Test the installation

On the command line, type `hadoop fs -ls gs://<some-bucket>`, where `<some-bucket>` is the Google Cloud Storage bucket to which you gave the connector read access. The command should output the top-level directories and objects contained in `<some-bucket>`. If there is a problem, see Troubleshooting the installation.

## Troubleshooting the installation

* If the installation test reported `No FileSystem for scheme: gs`, make sure that you correctly set the two properties in the correct core-site.xml.
* If the test reported `java.lang.ClassNotFoundException: com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`, check that you added the connector to the [Hadoop classpath](https://cloud.google.com/hadoop/google-cloud-storage-connector#classpath).
* If the test issued a message related to authorization, make sure that you have access to `<some-bucket>` with [`gsutil`](https://cloud.google.com/storage/docs/gsutil), and that the credentials in your configuration are correct.
