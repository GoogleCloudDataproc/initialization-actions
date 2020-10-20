--------------------------------------------------------------------------------

# NOTE: *The Docker initialization action has been deprecated. Please use the Docker Component*

**The
[Docker Component](https://cloud.google.com/dataproc/docs/concepts/components/docker)
is the best way to use Docker with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Docker Initialization Action

This initialization action installs a binary release of
[Docker](https://www.docker.com/) on a [Google Cloud
Dataproc](https://cloud.google.com/dataproc) cluster. After installation, it
will add the `yarn` user to the special `docker` group so that YARN-executed
applications can access Docker.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

1. Use the `gcloud` command to create a new cluster with this initialization
   action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/docker/docker.sh
    ```

1. Docker is installed and configured on all nodes of the cluster (both master
   and workers). You can log into the master node and run a test command to see
   that it works:

    ```bash
    sudo docker run hello-world
    ```

    Or, to run as the `yarn` user would:

     ```bash
     sudo su yarn
     docker run hello-world
     ```
