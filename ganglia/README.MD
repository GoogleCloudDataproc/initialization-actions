# Ganglia

This initialization action installs [Ganglia](http://ganglia.info/), a scalable monitoring system.

## Using this initialization action

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/ganglia/ganglia.sh
    ```

1. Once the cluster has been created, Ganglia is served on port `80` on the master node at `/ganglia`. To connect to the Ganglia web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. In the opened web browser, go to `http://CLUSTER_NAME-m/ganglia` on Standard/Single Node clusters, or `http://CLUSTER_NAME-m-0/ganglia` on High Availability clusters.
