# Dr. Elephant

This script installs [Dr. Elephant](https://github.com/linkedin/dr-elephant) on
dataproc clusters.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Dr.
Elephant installed.

```bash
gcloud dataproc clusters ${CLUSTER_NAME} \
    --initialization-actions gs://$MY_BUCKET/dr-elephant/dr-elephant.sh
```

Once the cluster has been created, Dr. Elephant is configured to run on port
`8080` and can be accessed by following instructions in
[connecting to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces).

Your jobs statistics should be there.
