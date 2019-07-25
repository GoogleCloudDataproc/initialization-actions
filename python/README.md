# Python setup and configuration tools

## Overview

## pip install packages

Install pip packages into user python environment. The full list of packages is specified
as `PIP_PACKAGES` metadata key. Packages are space separated and can contain version selectors.

Note: when using this initialization action with automation, pinning package versions
(see Example 2) is strongly encouraged to make cluster environments hermetic.

Example 1: installing one package at head

```
gcloud dataproc clusters create my-cluster \
    --metadata 'PIP_PACKAGES=pandas' \
    --initialization-actions gs://$MY_BUCKET/python/pip-install.sh
```

Example 2: installing several packages with version selectors

```
gcloud dataproc clusters create my-cluster \
    --metadata 'PIP_PACKAGES=pandas==0.23.0 scipy==1.1.0' \
    --initialization-actions gs://$MY_BUCKET/python/pip-install.sh
```

## conda install packages

Install conda packages into user python environment. The full list of packages is specified
as `CONDA_PACKAGES` metadata key. Packages are space separated and can contain version selectors.

Example 1: installing one package at head

```
gcloud dataproc clusters create my-cluster \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://$MY_BUCKET/python/conda-install.sh
```

Example 2: installing several packages with version selectors

```
gcloud dataproc clusters create my-cluster \
    --metadata 'CONDA_PACKAGES=scipy=0.15.0 curl=7.26.0' \
    --initialization-actions gs://$MY_BUCKET/python/conda-install.sh
```
