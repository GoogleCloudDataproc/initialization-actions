# OpenSSL Upgrade

This [initialization action] (https://cloud.google.com/dataproc/init-actions) installs OpenSSL 1.0.2
from Jessie-backports for Dataproc clusters running Dataproc 1.0 through 1.2.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with the backports version
of OpenSSL using the following instructions:

1. Uploading a copy of this initialization action (`openssl.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
2. Using the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `<CLUSTER_NAME>` and specify the initialization action stored in `<GCS_BUCKET>`

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/openssl.sh
    ```
## Important Notes

* This init action should not be applied to Dataproc image versions greater than
  1.2.
