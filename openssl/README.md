# OpenSSL Upgrade

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs OpenSSL 1.0.2
from Jessie-backports for Dataproc clusters running Dataproc 1.0 through 1.2
with debian 8. This init action is unnecessary on debian 9.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with the backports version
of OpenSSL using the following command:

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/openssl/openssl.sh
```

## Important notes

* This init action should not be applied to Dataproc image versions greater than
  1.2.
