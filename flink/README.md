--------------------------------------------------------------------------------

# NOTE: *The Flink initialization action has been deprecated. Please use the Flink Component*

**The
[Flink Component](https://cloud.google.com/dataproc/docs/concepts/components/flink)
is the best way to use Flink with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Apache Flink Initialization Action

This initialization action installs a binary release of
[Apache Flink](http://flink.apache.org) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
Additionally, this script will tune some basic parameters for Flink and start a
Flink session running on YARN.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/flink/flink.sh
    ```

1.  You can log into the master node of the cluster to submit jobs to Flink.
    Flink is installed in `/usr/lib/flink` (unless you change the setting) which
    contains a `bin` directory with Flink. **Note:** you need to specify
    `HADOOP_CONF_DIR=/etc/hadoop/conf` before your Flink commands for them to
    execute properly.

To run a job on an existing YARN session, run:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster \
    -yid $SESSION_APP_ID $JOB_JAR
```

To run a job on a transient session using `N` containers:

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster \
    -yn $N $JOB_JAR
```

The above command creates a transient cluster with the default, pre-configured
amount of JobManager and TaskManager memory. If you're using a non-standard
deployment (e.g., a single-node cluster) or want to run multiple transient
sessions concurrently on the same cluster, you will need to specify an
appropriate memory allocation. Note that `N`, the number of containers,
corresponds to the number of TaskManagers that will be created. An additional
container will be created for the JobManager.

```bash
HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run -m yarn-cluster \
    -yn $N -ynm $JOB_MANAGER_MEMORY_MB -ytm $TASK_MANAGER_MEMORY_MB $JOB_JAR
```

For example, this command will run a word count sample (as root) on an existing
YARN session:

```bash
sudo su - HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run \
    -m yarn-cluster -yid $SESSION_APP_ID examples/streaming/WordCount.jar
```

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

*   By default, a detached Flink YARN session is started for you. To find its
    application id, run `yarn application -list`.
*   The default session is configured to consume all YARN resources. If you want
    to submit multiple jobs in parallel or use transient sessions, you'll need
    to disable this default session. You can either fork this init script and
    set START_FLINK_YARN_SESSION_DEFAULT to `false`, or set the cluster metadata
    key `flink-start-yarn-session` to `false` when you create your cluster.
*   By default, flink is installed via apt. However, a custom flink snapshot can
    be installed by specifying the `flink-snapshot-url` metadata key to a URL
    that points to a valid flink tarball.
*   When creating **HA cluster** with Flink please add cluster property
    `--properties 'yarn:yarn.resourcemanager.am.max-attempts=4'` to enable HA
    YARN session. You can find more information in the Flink
    [documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/jobmanager_high_availability.html#yarn-cluster-high-availability).
