# Apache Flink Initialization Action

This initialization action installs a binary release of [Apache Flink](http://flink.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Additionally, this script will tune some basic parameters for
Flink and start a Flink session running on YARN.

## Using this initialization action

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/flink/flink.sh
    ```

1. You can log into the master node of the cluster to submit jobs to Flink. Flink is installed in `/usr/lib/flink` (unless you change the setting) which contains a `bin` directory with Flink. **Note** - you need to specify `HADOOP_CONF_DIR=/etc/hadoop/conf` before your Flink commands for them to execute properly.

To run a job on an existing YARN session, run:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yid <session application id> <job jar>
```

To run a job on a transient session using `N` containers:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yn N <job jar>
```

The above command creates a transient cluster with the default, pre-configured amount of JobManager and TaskManager memory. If you're using a non-standard deployment (e.g., a single-node cluster) or want to run multiple transient sessions concurrently on the same cluster, you will need to specify an appropriate memory allocation. Note that `N`, the number of containers, corresponds to the number of TaskManagers that will be created. An additional container will be created for the JobManager.

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yn N  -ynm <job manager memory (MB)> -ytm <task manager memory (MB)> <job jar>
```

For example, this command will run a word count sample (as root) on an existing YARN session:
```bash
sudo su - HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster -yid <session application id> examples/streaming/WordCount.jar
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

* By default, a detached Flink YARN session is started for you. To find its application id, run `yarn application -list`.
* The default session is configured to consume all YARN resources. If you want to submit multiple jobs in parallel or use transient sessions, you'll need to disable this default session. You can either fork this init script and set START_FLINK_YARN_SESSION_DEFAULT to `false`, or set the cluster metadata key `flink-start-yarn-session` to `false` when you create your cluster.
* By default, flink is installed via apt.  However, a custom flink snapshot can be installed by specifying the `flink-snapshot-url` metadata key to a URL that points to a valid flink tarball.
