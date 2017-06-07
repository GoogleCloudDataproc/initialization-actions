# Apache Flink Initialization Action

This initialization action installs a binary release of [Apache Flink](http://flink.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Additionally, this script will tune some basic parameters for
Flink and start a Flink session running on YARN.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Flink installed by:

1. Uploading a copy of the initialization action (`flink.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/flink.sh   
    --initialization-action-timeout 5m
    ```
1. 1. You can log into the master node of the cluster to submit jobs to Flink. Flink is installed in `/usr/lib/flink` (unless you change the setting) which contains a `bin` directory with Flink. **Note** - you need to specify `HADOOP_CONF_DIR=/etc/hadoop/conf` before your Flink commands for them to execute properly. 

To run a job on an existing YARN session, run:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yid <session application id> <job jar>
```

To run a job on a transient session using `N` containers:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yn N <job jar>
```

For example, this command will run a word count sample (as root) on an existing YARN session:
```bash
sudo su - HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yid <session application id> examples/streaming/WordCount.jar
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* By default, a detached Flink YARN session is started for you. To find its application id, run `yarn application -list`.
* The default session is configured to consume all YARN resources. If you want to submit multiple jobs in parallel or use transient sessions, you'll need to disable this default session. You can either fork this init script and set START_FLINK_YARN_SESSION_DEFAULT to `false`, or set the cluster metadata key `flink-start-yarn-session` to `false` when you create your cluster.
