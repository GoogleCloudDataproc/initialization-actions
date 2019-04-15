# Installing the connector

To install the connector manually, complete the following steps.

## Ensure authenticated Cloud Storage access

Depending on where the machines which comprise your cluster are located, you
must do one of the following:

*   **Google Cloud Platform** - Each Google Compute Engine VM must be
    [configured to have access](https://cloud.google.com/compute/docs/authentication#using)
    to the
    [Cloud Storage scope](https://cloud.google.com/storage/docs/authentication#oauth)
    you intend to use the connector for.
*   **non-Google Cloud Platform** - Obtain an
    [OAuth 2.0 private key](https://cloud.google.com/storage/docs/authentication#generating-a-private-key).
    Installing the connector on a machine other than a GCE VM can lead to higher
    Cloud Storage access costs. For more information, see
    [Cloud Storage Pricing](https://cloud.google.com/storage/pricing).

## Add the connector jar to Hadoop's classpath

Placing the connector jar in the `$HADOOP_COMMON_LIB_JARS_DIR` directory should
be sufficient to have Hadoop load the jar. Alternatively, to be certain that the
jar is loaded, you can add
`HADOOP_CLASSPATH=$HADOOP_CLASSPATH:</path/to/gcs-connector.jar>` to
`hadoop-env.sh` in the Hadoop configuration directory.

## Configure Hadoop

To begin, you will need a JSON keyfile so the connector can authenticate to
Google Cloud Storage. You can follow
[these directions](https://cloud.google.com/storage/docs/authentication#service_accounts)
to obtain a JSON keyfile.

Once you have the JSON key file, you need to configure following properties in
`core-site.xml` on your server:

```xml
<property>
  <name>fs.AbstractFileSystem.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  <description>The AbstractFileSystem for gs: uris.</description>
</property>
<property>
  <name>fs.gs.project.id</name>
  <value></value>
  <description>
    Optional. Google Cloud Project ID with access to GCS buckets.
    Required only for list buckets and create bucket operations.
  </description>
</property>
<property>
  <name>google.cloud.auth.service.account.enable</name>
  <value>true</value>
  <description>
    Whether to use a service account for GCS authorization.
    If set to `false` then GCE VM metadata service will be used for
    authorization.
  </description>
</property>
<property>
  <name>google.cloud.auth.service.account.json.keyfile</name>
  <value>/path/to/keyfile</value>
  <description>
    The JSON key file of the service account used for GCS
    access when google.cloud.auth.service.account.enable is true.
  </description>
</property>
```

You can alternatively set the environment variable
`GOOGLE_APPLICATION_CREDENTIALS` to your keyfile (`/path/to/keyfile.json`).

Additional properties can be specified for the Cloud Storage connector,
including alternative authentication options. For more information, see the
values documented in the [gcs-core-default.xml](/gcs/conf/gcs-core-default.xml)
file inside the `conf` directory.

## Configure Spark

Spark may not install a Hadoop `core-site.xml` in its `conf` dir, so you may
need to create one or, alternatively, set `spark.hadoop.*` properties in the
`spark-defaults.conf` file (see
[Custom Hadoop/Hive Configuration](https://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration)).

For example, instead of setting the required properties in `core-site.xml`, you
can set the properties in `spark-defaults.conf`:

```
spark.hadoop.google.cloud.auth.service.account.enable       true
spark.hadoop.google.cloud.auth.service.account.json.keyfile <path/to/keyfile.json>
```

## Test the installation

On the command line, type `hadoop fs -ls gs://<some-bucket>`, where
`<some-bucket>` is the Google Cloud Storage bucket to which you gave the
connector read access. The command should output the top-level directories and
objects contained in `<some-bucket>`. If there is a problem, see Troubleshooting
the installation.

## Troubleshooting the installation

*   If the installation test reported `No FileSystem for scheme: gs`, make sure
    that you correctly set the two properties in the correct `core-site.xml`.
*   If the test reported `java.lang.ClassNotFoundException:
    com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`, check that you added
    the connector to the Hadoop/Spark classpath.
*   If the test issued a message related to authorization, make sure that you
    have access to Cloud Storage using
    [gsutil](https://cloud.google.com/storage/docs/gsutil) (`gsutil
    gs://some-bucket`), and that the credentials in your configuration are
    correct.
