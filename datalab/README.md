--------------------------------------------------------------------------------

# NOTE: *The Datalab initialization action has been deprecated. Please use the Jupyter Component*

**The
[Jupyter Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
is the best way to use Jupyter with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Datalab Initialization Action

This initialization action downloads and runs a
[Google Cloud Datalab](https://cloud.google.com/datalab/) Docker container on a
Dataproc cluster. You will need to connect to Datalab using an SSH tunnel.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/datalab/datalab.sh \
        --scopes cloud-platform
    ```

1.  Once the cluster has been created, Datalab is configured to run on port
    `8080` on the master node in a Dataproc cluster. To connect to the Datalab
    web interface, you will need to create an SSH tunnel and use a SOCKS5 proxy
    as described in the
    [Dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
    documentation.

1.  Once you bring up a notebook, you should have the normal PySpark environment
    configured with `sc`, `sqlContext`, and `spark` predefined.

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Python 3

Datalab (and the Spark driver) can run with Python 2 or Python 3. However,
workers (executors) are configured to use Python 2. To change worker python, use
the
[Conda init action](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/conda).
Note that the driver (`PYSPARK_DRIVER_PYTHON`) and executors (`PYSPARK_PYTHON`)
must be at the same minor version. Currently, Datalab uses Python 3.5. Here is
how to set up Python 3.5 on workers:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_PACKAGES="python==3.5"' \
    --scopes cloud-platform \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/conda/bootstrap-conda.sh,gs://goog-dataproc-initialization-actions-${REGION}/conda/install-conda-env.sh,gs://goog-dataproc-initialization-actions-${REGION}/datalab/datalab.sh
```

In effect, this means that a particular Datalab-on-Dataproc cluster can only run
Python 2 or Python 3 kernels, but not both.

## Important notes

*   PySpark's
    [`DataFrame.toPandas()`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.toPandas)
    method is useful for integrating with Datalab APIs.
    *   Remember that Panda's DataFrames must fit on the master, whereas Spark's
        can fill a cluster.
    *   Datalab has a number of notebooks documenting its
        [Pandas](http://pandas.pydata.org/)' integrations.
*   This script requires that Datalab run on port `:8080`. If you normally run
    another server on that port (e.g. Zeppelin), consider moving it. Note
    running multiple Spark sessions can consume a lot of cluster resources and
    can cause problems on moderately small clusters.
*   If you
    [build your own Datalab images](https://github.com/googledatalab/datalab/wiki/Development-Environment),
    you can specify `--metadata docker-image=gcr.io/<PROJECT>/<IMAGE>` to point
    to your image.
*   If you normally only run Datalab kernels on VMs and connect to them with a
    local Docker frontend, set the flag
    `--metadata docker-image=gcr.io/cloud-datalab/datalab-gateway` and then set
    `GATEWAY_VM` to your cluster's master node in your local `docker` command
    [as described here](https://cloud.google.com/datalab/docs/quickstarts/quickstart-gce#install_the_datalab_docker_container_on_your_computer).
*   You can pass Spark packages as a comma separated list with `--metadata
    spark-packages=<PACKAGES>` e.g. `--metadata
    '^#^spark-packages=com.databricks:spark-avro_2.11:3.2.0,graphframes:graphframes:0.3.0-spark2.0-s_2.11`.
*   This init action runs Datalab in Docker, and installs Docker via the
    [Docker init action](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/docker).
    To run this script with a modified Docker init action, pass `--metadata
    "INIT_ACTIONS_REPO=gs://my-forked-dataproc-initialization-actions"`, or
    `--metadata
    "INIT_ACTIONS_REPO=https://github.com/myfork/dataproc-initialization-actions"` 
    and `--metadata "INIT_ACTIONS_BRANCH=branch-on-my-fork"`
