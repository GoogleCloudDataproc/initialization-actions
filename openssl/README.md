# OpenSSL Upgrade

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs OpenSSL 1.0.2
from Jessie-backports for Dataproc clusters running Dataproc 1.0 through 1.2
with debian 8. This init action is unnecessary on debian 9.

## Using this initialization action

**:warning: WARNING:** See [best practices](README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with the backports version
of OpenSSL using the following command:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/openssl/openssl.sh
```

## Important notes

* This init action should not be applied to Dataproc image versions greater than
  1.2.
