# Apache Toree Kernel

This init action installs the [Apache Toree](https://toree.apache.org/) kernel for
Jupyter notebooks.

This init action assumes that the Jupyter optional component and the `toree` pip package
are both installed in the cluster.

## Usage

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can create a cluster with Jupyter and the Apache Toree kernel configured using a
command like the following:

### Dataproc 2.0 (and later) images

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME?} \
  --region ${REGION?} \
  --image-version=2.0 \
  --enable-component-gateway \
  --optional-components=JUPYTER \
  --properties=dataproc:pip.packages='toree==0.5.0' \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION?}/toree/toree.sh
```

### Dataproc 1.5 images

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME?} \
  --region ${REGION?} \
  --image-version=1.5 \
  --enable-component-gateway \
  --optional-components=ANACONDA,JUPYTER \
  --properties=dataproc:pip.packages='toree==0.5.0' \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION?}/toree/toree.sh
```

