# Apache Flink Initialization Action

This initialization action installs a binary release of [Apache Flink](http://flink.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Additionally, this script will tune some basic parameters for
Flink and start a Flink session running on YARN.

## Using this initialization action
To use this initialization action, you will likely need to configure a few settings in the initialization action. Specifically, you shoud modify the following variables to match your desired setup:

* `SCALA_VERSION` - The Scala version installed on the Cloud Dataproc cluster
* `HADOOP_VERSION` - The Hadoop version installed on the Cloud Dataproc cluster
* `FLINK_VERSION` - The Flink version to be installed

The `SCALA_VERSION` and `HADOOP_VERSION` will be based on the [Cloud Dataproc image version](https://cloud.google.com/dataproc/concepts/dataproc-versions) you select. The `NUM_WORKERS` will be based on the number of workers you add to your cluster.

Once you have configured a copy of this script, you can use this initialization action to create a new Dataproc cluster with Flink installed by:

1. Uploading a copy of the initialization action (`flink.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/flink.sh   
    --initialization-action-timeout 5m
    ```
1. Once the cluster has been created, Flink will start a session on YARN. You can log into the master node of the cluster to submit jobs to Flink. Flink is installed in `/usr/lib/flink` (unless you change the setting) which contains a `bin` directory with Flink. **Note** - you need to specify `HADOOP_CONF_DIR=/etc/hadoop/conf` before your Flink commands for them to execute properly.

For example, this command will run a word count sample (as root):

`sudo su -
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster examples/streaming/WordCount.jar`

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script must be updated based on which Flink version you wish you install
* This script must be updated based on your Cloud Dataproc cluster
