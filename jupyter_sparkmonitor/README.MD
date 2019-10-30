# Jupyter Notebook with SparkMonitor

This folder contains the initialization action `sparkmonitor.sh` to quickly setup and
launch [Jupyter Notebook](http://jupyter.org/) with 
[SparkMonitor](https://krishnan-r.github.io/sparkmonitor/) to show Spark UI inside your notebook.

Note: This init action uses Conda and Python 3. 

__Pre-requisites:__
This initialization action uses the [Jupyter Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
which requires Cloud Dataproc image version 1.3 and later. The Jupyter Optional
Component's web interface can be accessed via
[Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
without using SSH tunnels. Also, you will need Anaconda as another Optional Component.


## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with
SparkMonitor installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster named
    `<CLUSTER_NAME>`.

    ```bash
    # Simple one-liner; just use all default settings for your cluster.
    # Jupyter will run on port 8123 of your master node.
    CLUSTER=<CLUSTER_NAME>
    gcloud dataproc clusters create $CLUSTER \
        --optional-components ANACONDA,JUPYTER --enable-component-gateway \
        --initialization-actions gs://$MY_BUCKET/jupyter_sparkmonitor/sparkmonitor.sh
    ```

1.  To access to the Jupyter web interface, you can just use the Component Gatway on the
GCP Dataproc cluster console. Alternatively, you can access following the instructions in
    [connecting to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces).

## Internal details

### sparkmonitor.sh

`sparkmonitor.sh` handles installing SparkMonitor, configuring and running Jupyter
on the Dataproc master node by doing the following:

-   Check to see if Conda and Jupyter Optional Components are installed. Fail if not.
-   Installing SparkMonitor using Python Pip
-   Enable the SparkMonitor as Jupyter Extension and configure IPython to load it
-   Configure Jupyter PySpark Kernel to use SparkMonitor
    -   Remove existing *PySpark* kernel under`jupyter/kernels/pyspark/kernel.json`
    -   Generate a new *PySpark* kernel and add to the same location
    -   Refresh Jupyter config and restart Jupyter service

## Important notes

*   This initialization action requires that you launch the Dataproc cluster with Jupyter and Conda optional components. 
The creating process will fail if it does not found them.