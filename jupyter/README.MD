# Jupyter Notebook

This folder contains the initialization action `jupyter.sh` to quickly setup and launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython notebook) along with [notebook extensions](https://github.com/ipython-contrib/IPython-notebook-extensions), and [RISE](https://github.com/damianavila/RISE). Jupyter is configured, by default, to run on port `8123` on the master node in the Dataproc cluster. The port can be overridden by setting the *metadata* property `JUPYTER_PORT` to a valid port number during cluster creation.

## jupyter.sh

`jupyter.sh` handles configuring and running Jupyter on the Dataproc master node by doing the following:

- clones the dataproc-initialization-actions git repo/branch specified in the `INIT_ACTIONS_REPO` and `INIT_ACTIONS_BRANCH` metadata keys
  - if these two metadata keys are not set during cluster creation, the default values `https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git` and `master` are used
  - this is provided so that a fork/branch of the main repo can easily be used, eg, during development
- executes `conda/bootstrap-conda.sh` and `conda/install-conda.sh` from said repo/branch to ensure `miniconda` is available
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
