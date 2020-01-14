# Ganglia

This initialization action installs [Ganglia](http://ganglia.info/), a scalable monitoring system.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/ganglia/ganglia.sh
    ```

1. Once the cluster has been created, Ganglia is served on port `80` on the master node at `/ganglia`. To connect to the Ganglia web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. In the opened web browser, go to `http://CLUSTER_NAME-m/ganglia` on Standard/Single Node clusters, or `http://CLUSTER_NAME-m-0/ganglia` on High Availability clusters.
