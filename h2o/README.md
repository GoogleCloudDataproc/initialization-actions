# H2O Sparkling Water Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs
[H2O Sparkling Water](http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/deployment/sw_google_cloud_dataproc.html)
on all nodes of [Google Cloud Dataproc](https://cloud.google.com/dataproc)
cluster.

This initialization works with Dataproc image version `1.3` and newer, except
`1.5` image.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with H2O
Sparkling Water installed:

1.  To create Dataproc 1.3 cluster use `conda` initialization action:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --image-version 1.3 \
        --scopes "cloud-platform" \
        --initialization-actions "gs://goog-dataproc-initialization-actions-${REGION}/conda/bootstrap-conda.sh,gs://goog-dataproc-initialization-actions-${REGION}/h2o/h2o.sh"
    ```

1.  To create Dataproc 1.4 cluster use `ANACONDA` optional component:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --image-version 1.4 \
        --optional-components ANACONDA \
        --scopes "cloud-platform" \
        --initialization-actions "gs://goog-dataproc-initialization-actions-${REGION}/h2o/h2o.sh"
    ```

1.  To create Dataproc 2.0 cluster and newer you don't need any additional
    initialization actions or optional components:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --image-version 2.0 \
        --scopes "cloud-platform" \
        --initialization-actions "gs://goog-dataproc-initialization-actions-${REGION}/h2o/h2o.sh"
    ```

Submit sample job:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    "gs://goog-dataproc-initialization-actions-${REGION}/h2o/sample-script.py"
```

### Supported metadata parameters

*   `H2O_SPARKLING_WATER_VERSION`: Sparkling Water version number. You can find
    the versions from the
    [releases](https://github.com/h2oai/sparkling-water/releases) page on
    GitHub. Default is `3.30.1.2-1`.

