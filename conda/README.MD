# Miniconda (a Python distro, along with package and environment manager, from Continuum Analytics)

## Overview

This folder contains initialization actions for use Miniconda / conda, who can be introduced as:
 
- [Minconda](http://conda.pydata.org/miniconda.html), a barebones version of [Anaconda](https://www.continuum.io/why-anaconda) of an open source (and totally legit) Python distro from Continuum Analytics, alongside,
- [conda](http://conda.pydata.org/docs/), an open source (and amazing) package and environment management system.

This allows Dataproc users to quickly and easily provision a Dataproc cluster leveraging conda's powerful management capabilities, by specifying a list of conda and / or pip packages along with use of [conda environment definitions](https://github.com/conda/conda-env#environment-file-example). All configuration is exposed via environment variables set to sane point-and-shoot defaults.

An example, to be used in a bash shell script:

```
gcloud dataproc clusters create my-dataproc-cluster \
    --initialization-actions \
        gs://dataproc-initialization-actions/jupyter/create-my-cluster.sh \
    --bucket my-dataproc-bucket \
    --num-workers 2 \
    --worker-machine-type=n1-standard-4 \
    --master-machine-type=n1-standard-4
```

Where `create-my-cluster.sh` specifies a list of conda and / or pip packages to install:

```
# Install Miniconda / conda
./dataproc-initialization-actions/conda/bootstrap-conda.sh
# Update conda root environment with specific packages in pip and conda
CONDA_PACKAGES='pandas dash scikit-learn'
PIP_PACKAGES='plotly cufflinks'
CONDA_PACKAGES=$CONDA_PACKAGES PIP_PACKAGES=$PIP_PACKAGES ./dataproc-initialization-actions/conda/install-conda-env.sh
```

Similarly, one can also specify a [conda environment yml file](https://github.com/conda/conda-env):

```
#!/usr/bin/env bash

CONDA_ENV_YAML_GSC_LOC="gs://my-bucket/path/to/conda-environment.yml"
CONDA_ENV_YAML_PATH="/root/conda-environment.yml"
echo "Downloading conda environment at $CONDA_ENV_YAML_GSC_LOC to $CONDA_ENV_YAML_PATH ... "
gsutil -m cp -r $CORAL_ENV_YAML_GSC_LOC $CONDA_ENV_YAML_PATH

# Install Miniconda / conda
./dataproc-initialization-actions/conda/bootstrap-conda.sh
# Create / Update conda environment via conda yaml
CONDA_ENV_YAML=$CONDA_ENV_YAML_PATH ./dataproc-initialization-actions/conda/install-conda-env.sh

```


## Details 

### bootstrap-conda.sh

`bootstrap-conda.sh` contains logic for quickly configuring and installing Miniconda across the dataproc cluster. Defaults to [`Miniconda3-latest-Linux-x86_64.sh`](https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh) package (e.g., Python 3), however, users can easily config for targeted versions via the following env vars (see script source for all options):
    - `MINICONDA_VARIANT`: the Python version can be `2` or `3`
    - `MINICONDA_VER`: the Miniconda version (e.g., `4.0.5`)
In addition, the script:
- downloads and installs Miniconda to the `$HOME` directory
- updates `$PATH`, exposing conda across all shell processes (for both interactive and batch sessions)
- installs some useful extensions:
    - [`conda-build`](https://github.com/conda/conda-build)
    - [`anaconda-client`](https://github.com/Anaconda-Server/anaconda-client)
    
See the script source for more options on configuration. :)

### install-conda-env.sh

`install-conda-env.sh` contains logic for creating a conda environment and installing conda and/or pip packages. Defaults include:

- if no conda environment name is specified, uses `root` (recommended).
- detects if conda environment has already been created.
- updates `/etc/profile` to activate the created environment at login (if needed)

Note: When creating a conda environment using an environment.yml (via setting .yml path in `CONDA_ENV_YAML`), the `install-conda-env.sh` script simply *updates the **root** environment* with dependencies specified in the file (i.e., ignoring the `name:` key). This sidesteps some conda issues in `source activate`ing, while still providing all dependencies across the Dataproc cluster.

If nothing else you need to include the bootstrap file as well eg.: 
gcloud dataproc clusters create foo --initialization-actions gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://dataproc-initialization-actions/conda/install-conda-env.sh


### Notes on running Python 3

Starting with Python 3.3, hash randomization is enabled by default (see docs for [object.\__hash__](https://docs.python.org/3/reference/datamodel.html#object.__hash__)). Spark attempts to correct this by setting the `PYTHONHASHSEED` environment variable, but a [small bug in Spark](https://issues.apache.org/jira/browse/SPARK-13330) keeps the env var from propogating to all executors.

The `boostrap-conda.sh` initialization action fixes this issue; users will not have to take any additional actions to use Python 3 on Dataproc. Alternatively, users can pass the following `properties` argument when creating a DataProc cluster:
```
gcloud dataproc clusters create --properties spark:spark.executorEnv.PYTHONHASHSEED=0 ...
```
Note that at this time Dataproc will *NOT* accept the property value when submitting a job; it must be passed when the cluster is created.


## Testing Installation

A quick test to ensure a correct installation of conda, we can submit jobs that collect distinct paths to the Python distribution across all Spark executors. For both local (e.g., running from dataproc cluster master node) and remote (e.g. submitting a job via the dataproc API) jobs, the result should be a list with a single path: `['/usr/local/bin/miniconda/bin/python']`. For example


### Local Job Test

After sshing to master node (e.g., `gcloud compute ssh $DATAPROC_CLUSTER_NAME-m`), run the `get-sys-exec.py` script contained in this directory:

```bash
> spark-submit get-sys-exec.py
... # Lots of output
['/usr/local/bin/miniconda/bin/python']
...
```

### Remote Job Test

From command line of local / host machine, one can submit remote job:

```bash
> gcloud dataproc jobs submit pyspark --cluster $DATAPROC_CLUSTER_NAME get-sys-exec.py
... # Lots of output
['/usr/local/bin/miniconda/bin/python']
...
```
