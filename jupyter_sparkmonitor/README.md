--------------------------------------------------------------------------------

# NOTE: *The Jupyter Spark Monitor initialization action has been deprecated. Please use the Jupyter Component*

**The
[Jupyter Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
is the best way to use Jupyter Spark Monitor with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Jupyter Notebook with SparkMonitor

This folder contains the initialization action `sparkmonitor.sh` to quickly
setup and launch [Jupyter Notebook](http://jupyter.org/) with
[SparkMonitor](https://krishnan-r.github.io/sparkmonitor/) to show Spark UI
inside your notebook.

Note: This init action uses Conda and Python 3.

__Pre-requisites:__ This initialization action uses the
[Jupyter Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
which requires Cloud Dataproc image version 1.4 and later. The Jupyter Optional
Component's web interface can be accessed via
[Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
without using SSH tunnels. Also, you will need Anaconda as another Optional
Component.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
SparkMonitor installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    # Jupyter will run on port 8123 of your master node.
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ANACONDA,JUPYTER \
        --enable-component-gateway \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/jupyter_sparkmonitor/sparkmonitor.sh
    ```

1.  To access to the Jupyter web interface, you can just use the Component
    Gateway on the GCP Dataproc cluster console. Alternatively, you can access
    following the instructions in
    [connecting to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces).
1.  After open Jupyter notebook, make sure you select the `Python3` kernel
    instead of the `PySpark` kernel.
1.  Inside your notebook, you can get `SparkContext` and `SparkSession` using
    the following code
    ```
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession.builder.appName('YOUR APP NAME').getOrCreate()
    ```

## Internal details

### sparkmonitor.sh

`sparkmonitor.sh` handles installing SparkMonitor, configuring and running
Jupyter on the Dataproc master node by doing the following:

-   Check to see if Conda and Jupyter Optional Components installed. Fail if
    not.
-   Installing SparkMonitor using `pip`.
-   Enable the SparkMonitor as Jupyter Extension and configure IPython kernel
    to load the extension.
-   Refresh Jupyter config and restart Jupyter service.

## Important notes

*   This initialization action requires that you launch the Dataproc cluster
    with Jupyter and Conda optional components. The creating process will fail
    if it does not found them.
