# Jupyter Notebook

This folder contains the initialization action `jupyter.sh` to quickly setup and launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython notebook) and a script to be run on the user's local machine to access the Jupyter notebook server.

A real-world example of how to use this from within a bash script, using the default dataproc-initialization-actions repo/branch:

```bash
# Simple one-liner; just use all default settings for your cluster. Jupyter will run on port 8123
# of your master node.
gcloud dataproc clusters create my-dataproc-cluster \
    --initialization-actions \
        gs://dataproc-initialization-actions/jupyter/jupyter.sh \
```

There are various options for customizing your Jupyter installation. For example to change the port
on which the Jupyter server runs, you can set the metadata key `JUPYTER_PORT`. You may also want
to change the number of workers and machine type of your cluster:

```bash
# Override the Jupyter port with 8124 (default is 8123)
gcloud dataproc clusters create my-dataproc-cluster \
    --metadata "JUPYTER_PORT=8124" \
    --initialization-actions \
        gs://dataproc-initialization-actions/jupyter/jupyter.sh \
    --num-workers 2 \
    --properties spark:spark.executorEnv.PYTHONHASHSEED=0,spark:spark.yarn.am.memory=1024m \
    --worker-machine-type=n1-standard-4 \
    --master-machine-type=n1-standard-4
```
You can pre-install various Python packages through `conda` by specifying a colon-separated
list in the metadata entry `JUPYTER_CONDA_PACKAGES`:

```bash
gcloud dataproc clusters create my-dataproc-cluster \
    --metadata "JUPYTER_PORT=8124,JUPYTER_CONDA_PACKAGES=numpy:pandas:scikit-learn" \
    --initialization-actions \
        gs://dataproc-initialization-actions/jupyter/jupyter.sh \
    --num-workers 2 \
    --properties spark:spark.executorEnv.PYTHONHASHSEED=0,spark:spark.yarn.am.memory=1024m \
    --worker-machine-type=n1-standard-4 \
    --master-machine-type=n1-standard-4
```

## jupyter.sh

`jupyter.sh` handles configuring and running Jupyter on the Dataproc master node by doing the following:

- clones the dataproc-initialization-actions git repo/branch specified in the `INIT_ACTIONS_REPO` and `INIT_ACTIONS_BRANCH` metadata keys
  - if these two metadata keys are not set during cluster creation, the default values `https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git` and `master` are used
  - this is provided so that a fork/branch of the main repo can easily be used, eg, during development
- executes `conda/bootstrap-conda.sh` from said repo/branch to ensure `miniconda` is available
- executes `jupyter/internal/setup-jupyter-kernel.sh` and `jupyter/internal/launch-jupyter-kernel.sh` from said repo/branch
  - configures `jupyter` to use the *PySpark* kernel found at `jupyter/kernels/pyspark/kernel.json`
  - configures `jupyter` to listen on the port specified by the metadata key `JUPYTER_PORT`, with a default value of `8123`
  - targets a default notebooks directory `/root/notebooks`, into which the directory found at `gs://$DATAPROC_BUCKET/notebooks/` is copied, where `$DATAPROC_BUCKET` is the value stored in the metadata key `dataproc-bucket` (set by default upon cluster creation and overridable)
  - launches the `jupyter notebook` process

**NOTE**: to be run as an init action.


## launch-jupyter-interface.sh

`launch-jupyter-interface.sh` launches a web interface to connect to Jupyter notebook process running on master node.

- sets a path for the local OS to the Chrome executable
- setup an ssh tunnel and socks proxy to the master node
- launch a Chrome instance that uses this ssh tunnel and references the Jupyter port.

**NOTE**: to be run from a local machine
 
