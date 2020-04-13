# Python setup and configuration tools

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

## Install PyPI packages

Install [PyPI](https://pypi.org) packages into user Python environment using
`pip` command.

**Note:** when using this initialization action with automation, pinning package
versions (see examples) is strongly encouraged to make cluster environment
hermetic.

### Options

-   `PIP_PACKAGES` - a space separated list of packages to install. Packages can
    contain version selectors.

### Examples

Installing one package at head

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'PIP_PACKAGES=pandas' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
```

Installing several packages with version selectors

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'PIP_PACKAGES=pandas==0.23.0 scipy==1.1.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
```

## Install Conda packages

Install Conda packages into user Python environment using `conda` command.

**Note:** when using this initialization action with automation, pinning package
versions (see examples) is strongly encouraged to make cluster environment
hermetic.

### Options

-   `CONDA_CHANNELS` - a space separated list of new channels to configure in
    addition to default channels.
-   `CONDA_PACKAGES` - a space separated list of packages to install. Packages
    can contain version selectors.

### Examples

Installing one package at head

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh
```

Installing several packages with version selectors

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_PACKAGES=scipy=0.15.0 curl=7.26.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh
```

Installing packages from a new channel

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata 'CONDA_CHANNELS=bioconda' \
    --metadata 'CONDA_PACKAGES=recentrifuge=1.0.0' \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/conda-install.sh
```
