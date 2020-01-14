# Python setup and configuration tools

## Using this initialization action

**:warning: NOTICE:** See [best practices](README.md#how-initialization-actions-are-used) of using initialization actions in production.

## pip install packages

Install pip packages into user python environment. The full list of packages is specified
as `PIP_PACKAGES` metadata key. Packages are space separated and can contain version selectors.

Note: when using this initialization action with automation, pinning package versions
(see Example 2) is strongly encouraged to make cluster environments hermetic.

Example 1: installing one package at head

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'PIP_PACKAGES=pandas' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
```

Example 2: installing several packages with version selectors

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'PIP_PACKAGES=pandas==0.23.0 scipy==1.1.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
```

## conda install packages

Install conda packages into user python environment. The full list of packages is specified
as `CONDA_PACKAGES` metadata key. Packages are space separated and can contain version selectors.

Example 1: installing one package at head

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh
```

Example 2: installing several packages with version selectors

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_PACKAGES=scipy=0.15.0 curl=7.26.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh
```
