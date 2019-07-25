# Docker Initialization Action

This initialization action installs a binary release of
[Docker](https://www.docker.com/) on a [Google Cloud
Dataproc](https://cloud.google.com/dataproc) cluster. After installation, it
will add the `yarn` user to the special `docker` group so that YARN-executed
applications can access Docker.

## Using this initialization action

1. Use the `gcloud` command to create a new cluster with this initialization
   action. The following command will create a new cluster named
   `<CLUSTER_NAME>`:

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/docker/docker.sh
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
