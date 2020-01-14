# Customize environment

This initialization action customizes the environment of all current and future non-system users on the VM. If you wish to ssh into your cluster a lot and prefer to customize the user environment, this script provides a starting point reference.

By default it only enables the options already present in the .bashrc that Debian provides, but it documents where further changes can be made and gives some commented out examples.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action by:

1. Editing and uploading a copy of this initialization action (`user-environment.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/user-environment
