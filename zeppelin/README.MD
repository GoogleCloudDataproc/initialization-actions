# Apache Zeppelin

This initialization action installs the latest version of [Apache Zeppelin](https://zeppelin.apache.org/) on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Apache Zeppelin installed by:

1. Uploading a copy of this initialization action (`zeppelin.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/zeppelin.sh
    ```
1. Once the cluster has been created, Zeppelin is configured to run on port `8080` on the master node in a Dataproc cluster. To connect to the Apache Zeppelin web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes & Known issues
* The port Zeppelin runs on is configurable by passing `--metadata=zeppelin-port=<PORT>' to `gcloud`.
* This installs Zeppelin 0.5.6 in Dataproc 1.0 and Zeppelin 0.6.1 in Dataproc 1.1.
* It configures the BigQuery interpreter in 0.6.1.
* It makes no attempt to install or configure matplotlib, because it [requires an XServer](https://github.com/apache/zeppelin/blob/master/docs/interpreter/python.md#matplotlib-integration).
* By default it only install Rs graphing libraries that ship with Debian 8 (ggplot2, knitr, and googlevis).
  * To install the other required R libraries (mplot and rCharts), uncomment lines after "Uncomment here" in zeppelin.sh.
  * There are still some issues with examples in the R Tutorial (under investation).
* The Hive interpreter in missing in Dataproc 1.1 (under investigation).
