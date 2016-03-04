# HUE - Hadoop User Experience 

This [initialization action] (https://cloud.google.com/dataproc/init-actions) installs the latest version of [HUE](http://gethue.com/) 
on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Hue installed by:

1. Uploading a copy of this initialization action (`hue.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
2. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you will need to increase the `initialization-action-timeout` to at least 10 minutes to allow for Hue to compile and install. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 10 minutes.
   
    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/hue.sh   
    --initialization-action-timeout 10m
    ```
3. Once the cluster has been created, Hue is configured to run on port `8888` on the master node in a Dataproc cluster. To connect to the Hue web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. 


