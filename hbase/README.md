# Apache HBase
This script installs [Apache HBase](https://hbase.apache.org/) on dataproc clusters. Apache HBase is a distributed and scalable Hadoop database.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Apache HBase installed on every node:

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/hbase/hbase.sh \
        --num-masters 3 --num-workers 2
    ```
1. You can validate your deployment by ssh into any node and running:

    ```bash
    hbase shell
    ```

1. Apache HBase UI on the master node can be accessed after connecting with the command:
    ```bash
    gcloud compute ssh <CLUSTER_NAME>-m-0 -- -L 16010:<CLUSTER_NAME>-m-0:16010
    ```
    Then just open a browser and type `localhost:16010` address.

1. HBase running on dataproc can be easily scaled up. The following command will add three additional workers (RegionServers) to previously created cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters update <CLUSTER_NAME> --num-workers 5
    ```

## Important notes

- This initialization works with all cluster configuration on dataproc version 1.3 and 1.2, but it is intended to be used in the HA mode.
- In HA clusters HBase is using Zookeeper that is pre-installed on master nodes, on a single node and standard clusters Zookeeper that comes with HBase is being used.