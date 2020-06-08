--------------------------------------------------------------------------------

# NOTE: *The Jupyter initialization action has been deprecated. Please use the Jupyter Component*

**The
[Jupyter Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
is the best way to use Jupyter with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Jupyter Notebook

This folder contains the initialization action `jupyter.sh` to quickly setup and
launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython
notebook) and a script to be run on the user's local machine to access the
Jupyter notebook server.

Note: This init action uses Conda and Python 3. Python 2 and `pip` users should
consider using the
[jupyter2 action](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/jupyter2).

__Use the Dataproc
[Jupyter Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)__.
Clusters created with Cloud Dataproc image version 1.3 and later can install
Jupyter Notebook without using this initialization action. The Jupyter Optional
Component's web interface can be accessed via
[Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
without using SSH tunnels.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Jupyter installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    # Simple one-liner; just use all default settings for your cluster.
    # Jupyter will run on port 8123 of your master node.
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/jupyter/jupyter.sh
    ```

1.  Run `./launch-jupyter-interface` to connect to the Jupyter notebook running
    on the master node. This creates a SOCKS5 proxy to the master node and
    launches a Google Chrome window that uses this proxy. Note: you will need to
    edit the script to point it at the Chrome installation path for your
    operating system. Alternatively, follow the instructions in
    [connecting to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces).

### Options

There are various options for customizing your Jupyter installation. These can
be provided as metadata keys using `--metadata`.

*   `JUPYTER_PORT`=<integer>: Port on which the Jupyter server runs
*   `JUPYTER_CONDA_PACKAGES`=<colon-separated list of strings>: List of Conda
    packages to install.
*   `INIT_ACTIONS_REPO`=<bucket>: GCS bucket or GitHub repo to find other
    scripts to install/configure Conda and Jupyter.
*   `INIT_ACTIONS_BRANCH`=<string>: Branch in GitHub `INIT_ACTIONS_REPO` to use.

For example to specify a different port and specify additional packages to
install:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata "JUPYTER_PORT=8124,JUPYTER_CONDA_PACKAGES=numpy:pandas:scikit-learn" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/jupyter/jupyter.sh \
    --properties spark:spark.executorEnv.PYTHONHASHSEED=0,spark:spark.yarn.am.memory=1024m \
    --worker-machine-type n1-standard-4 \
    --master-machine-type n1-standard-4
```

Notebooks are stored and retrieved from the cluster staging bucket (Google Cloud
Storage) at `gs://<staging-bucket>/notebooks/`. By default, clusters in your
project in the same region use the same bucket. You can explicitly provide
`--bucket=gs://<some-bucket>` to the same value to share notebooks between them.

## Internal details

### jupyter.sh

`jupyter.sh` handles configuring and running Jupyter on the Dataproc master node
by doing the following:

-   clones the GCS bucket or GitHub repo specified in the `INIT_ACTIONS_REPO`
    and `INIT_ACTIONS_BRANCH` (for GitHub repo) metadata keys
    -   if `INIT_ACTIONS_REPO` metadata key is not set during cluster creation,
        the default value `gs://dataproc-initialization-actions` is used
    -   this is provided so that a fork of the main repo can easily be used, eg,
        during development
-   executes `conda/bootstrap-conda.sh` from said repo/branch to ensure
    `miniconda` is available
-   executes `jupyter/internal/setup-jupyter-kernel.sh` and
    `jupyter/internal/launch-jupyter-kernel.sh` from said repo/branch
    -   configures `jupyter` to use the *PySpark* kernel found at
        `jupyter/kernels/pyspark/kernel.json`
    -   configures `jupyter` to listen on the port specified by the metadata key
        `JUPYTER_PORT`, with a default value of `8123`
    -   configures `jupyter` to use auth token `JUPYTER_AUTH_TOKEN`, with a
        default of none.
        -   Dataproc does not recommend opening firewall ports to access
            Jupyter, but rather using a proxy. See
            [connecting to web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces)
        -   This proxy access is automated by
            [launch-jupyter-interfaces.sh](#launch-jupyter-interfacesh).
    -   loads and saves notebooks to `gs://$DATAPROC_BUCKET/notebooks/`, where
        `$DATAPROC_BUCKET` is the value stored in the metadata key
        `dataproc-bucket` (set by default upon cluster creation and
        overridable). Note that all clusters sharing a `$DATAPROC_BUCKET` will
        share notebooks.
    -   launches the `jupyter notebook` process

**NOTE**: to be run as an init action.

### launch-jupyter-interface.sh

`launch-jupyter-interface.sh` launches a web interface to connect to Jupyter
notebook process running on master node.

-   sets a path for the local OS to the Chrome executable
-   setup an ssh tunnel and socks proxy to the master node
-   launch a Chrome instance that uses this ssh tunnel and references the
    Jupyter port.

**NOTE**: to be run from a local machine

## Important notes

*   This initialization action clones `gs://goog-dataproc-initialization-actions-${REGION}` GCS
    bucket to run other scripts in the repo. If you plan to copy `jupyter.sh` to
    your own GCS bucket, you will also need to fork this repository and specify
    the `INIT_ACTIONS_REPO` metadata key.
*   This initialization action runs the conda init action, which supports the
    metadata keys `CONDA_PACKAGES` and `PIP_PACKAGES`. You can also use these to
    install additional packages.
