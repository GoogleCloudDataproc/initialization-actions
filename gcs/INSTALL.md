# Installing the connector

To install the connector manually, complete the following steps.

## Ensure authenticated Cloud Storage access

Depending on where the machines which comprise your cluster are located, you must do one of the following:

* **Google Cloud Platform** - Each Google Compute Engine VM must be [configured to have access](https://cloud.google.com/compute/docs/authentication#using) to the [Cloud Storage scope](https://cloud.google.com/storage/docs/authentication#oauth) you intend to use the connector for.
* **non-Google Cloud Platform** - Obtain an [OAuth 2.0 private key](https://cloud.google.com/storage/docs/authentication#generating-a-private-key). Installing the connector on a machine other than a GCE VM can lead to higher Cloud Storage access costs. For more information, see [Cloud Storage Pricing](https://cloud.google.com/storage/pricing).

## Add the connector jar to Hadoop's classpath

Placing the connector jar in the appropriate subdirectory of the Hadoop installation may be effective to have Hadoop load the jar. However, to be certain that the jar is loaded, add `HADOOP_CLASSPATH=$HADOOP_CLASSPATH:</path/to/gcs-connector-jar>` to `hadoop-env.sh` in the Hadoop configuration directory.

## Configure Hadoop

To begin, you will need a JSON keyfile so the connector can authenticate to Google Cloud Storage. You can follow [these directions](https://cloud.google.com/storage/docs/authentication#service_accounts) to obtain a JSON keyfile.

Once you have the JSON key file, you must add the following properties to `core-site.xml` on your server.

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
    <property>
      <name>fs.gs.project.id</name>
      <value></value>
      <description>
        Required. Google Cloud Project ID with access to configured GCS buckets.
      </description>
    </property>
    <property>
      <name>google.cloud.auth.service.account.enable</name>
      <value>true</value>
      <description>
        Whether to use a service account for GCS authorizaiton. If an email and
        keyfile are provided (see google.cloud.auth.service.account.email and
        google.cloud.auth.service.account.keyfile), then that service account
        willl be used. Otherwise the connector will look to see if it running on
        a GCE VM with some level of GCS access in it's service account scope, and
        use that service account.
      </description>
    </property>
    <property>
      <name>google.cloud.auth.service.account.json.keyfile</name>
      <value></value>
      <description>
        The JSON key file of the service account used for GCS
        access when google.cloud.auth.service.account.enable is true.
      </description>
    </property>

Additional properties can be specified for the Cloud Storage connector, including alternative authentication options. For more information, see the values documented in the [gcs-core-default.xml](/conf/gcs-core-default.xml) file inside the `conf` directory.

## Test the installation

On the command line, type `hadoop fs -ls gs://<some-bucket>`, where `<some-bucket>` is the Google Cloud Storage bucket to which you gave the connector read access. The command should output the top-level directories and objects contained in `<some-bucket>`. If there is a problem, see Troubleshooting the installation.

## Troubleshooting the installation

* If the installation test reported `No FileSystem for scheme: gs`, make sure that you correctly set the two properties in the correct core-site.xml.
* If the test reported `java.lang.ClassNotFoundException: com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`, check that you added the connector to the [Hadoop classpath](https://cloud.google.com/hadoop/google-cloud-storage-connector#classpath).
* If the test issued a message related to authorization, make sure that you have access to `<some-bucket>` with [`gsutil`](https://cloud.google.com/storage/docs/gsutil), and that the credentials in your configuration are correct.
