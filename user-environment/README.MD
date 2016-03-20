# Customize environment

This initialization action customizes the environment of all current and future non-system users on the VM. If you wish to ssh into your cluster a lot and prefer to customize the user environment, this script provides a starting point reference.

By default it only enables the options already present in the .bashrc that Debian provides, but it documents where further changes can be made and gives some commented out examples.

## Using this initialization action
You can use this initialization action by:

1. Uploading a copy of this initialization action (`user-environment.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`:
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/user-environment
