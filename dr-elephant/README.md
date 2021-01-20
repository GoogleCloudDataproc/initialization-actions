# Dr. Elephant

This script installs [Dr. Elephant](https://github.com/linkedin/dr-elephant) on
dataproc clusters.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Dr.
Elephant installed.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dr-elephant/dr-elephant.sh
```

Once the cluster has been created, Dr. Elephant is configured to run on port
`8080` and can be accessed by following instructions in
[connecting to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces).

Your jobs statistics should be there.

## Supported Spark Versions

As of Janurary 19th, 2021,
[Dr. Elephant only supports Spark < 2.2.3](https://github.com/linkedin/dr-elephant/issues/683)
