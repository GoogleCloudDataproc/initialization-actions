# Miniconda (a Python distro and package manager, from Continuum Analytics)

This folder contains initialization actions for getting up and going using [Miniconda](http://conda.pydata.org/miniconda.html). Miniconda is a shell script installer that contains both:
 
- [Anaconda](https://www.continuum.io/why-anaconda), a barebones version of a completely open (and legit) Python distro from Continuum Analytics, alongside,
- [conda](http://conda.pydata.org/docs/), a completely open (and amazing) package manager.
 

## bootstrap-conda.sh

`bootstrap-conda.sh` contains logic for quickly configuring and installing Miniconda, with point-and-shoot defaults, including:

- a choice of either Python 2 or Python 3 (defaults to Python 2; users can uncomment the appropriate lines near the top of `boostrap-conda.sh` to select Python 3)
- the most recent Miniconda version (`3.18.3`)
- downloads and installs to the `$HOME` directory
- performs an md5sum hash check (failing quickly is always better)
- updates current `$PATH` and subsequent definitions in `.bashrc`
- updates `conda` and install `pip` in root environment (allowing pip installations)
- installs some powerful extensions:
    - [`conda-env`](https://github.com/conda/conda-env)
    - [`conda-build`](https://github.com/conda/conda-build)
    - [`anaconda-client`](https://github.com/Anaconda-Server/anaconda-client)
    
 
All of this can be changed with little effort.

## install-conda-env.sh

`install-conda-env.sh` contains logic for creating a conda environment and installing conda (and pip) packages. Sane defaults include:

- installs some common libraries (pandas, scikit-learn, bokeh, plotly, Jupyter) 
- if no conda environment name is specified, uses `root`.
- detects if conda environment has already been created.
- updates `.bashrc` to activate the created environment at login

## Notes on running Python 3

Starting with Python 3.3, hash randomization is enabled by default (see docs for [object.\__hash__](https://docs.python.org/3/reference/datamodel.html#object.__hash__)). Spark attempts to correct this by setting the `PYTHONHASHSEED` environment variable, but a [small bug in Spark](https://issues.apache.org/jira/browse/SPARK-13330) keeps the env var from propogating to all executors.

The `boostrap-conda.sh` initialization action fixes this issue; users will not have to take any additional actions to use Python 3 on DataProc. Alternatively, users can pass the following `properties` argument when creating a DataProc cluster:
```
gcloud dataproc clusters create --properties spark:spark.executorEnv.PYTHONHASHSEED=0 ...
```
Note that at this time Dataproc will NOT accept the property value when submitting a job; it must be passed when the cluster is created.

## Testing Installation

A quick test to ensure installation of conda is working, we can submit jobs that collect distinct paths to the python distribution across all Spark executors. For both local (e.g., running form dataproc cluster master node) and remote (e.g. submitting a job via the dataproc API) jobs, the result should be a list with a single path: `['/usr/local/bin/miniconda/bin/python']`. See more


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
