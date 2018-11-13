# Prometheus
This script installs [Prometheus](https://prometheus.io/) on dataproc clusters. Prometheus is a time series database that allows visualizing, querying metrics gathered from different cluster components during job execution.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Prometheus installed on every node:

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/prometheus/prometheus.sh
    ```
1.  Prometheus UI on the master node can be accessed after connecting with the command:
    ```bash
    gcloud compute ssh <CLUSTER_NAME>-m -- -L 9090:<CLUSTER_NAME>-m:9090
    ```
    Then just open a browser and type `localhost:9090` address.

1. Prometheus UI on worker node can be accessed similarly, but just substitute `-m` suffix with `-w-0`,  `-w-1` depending on worker you would like to access. You can also [setup ssh tunnel and configure sock proxy](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to access all master nodes and worker nodes using their internal hostnames.

## Important notes
Prometheus uses StatsD to retrieve metrics, but the StatsD sink for metrics publishing is available on Apache Spark 2.3.0, so aggregating metrics from Spark on clusters with software version other than 1.3 will result in an error. 
StatsD sink for Apache Hadoop metrics is available on Hadoop Versions 2.8+ so will work on clusters with software version 1.2 or 1.3.