# HUE - Hadoop User Experience

This [initialization action] (https://cloud.google.com/dataproc/init-actions) installs the latest version of [HUE](http://gethue.com/)
on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Hue installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/hue/hue.sh
    ```

1. Once the cluster has been created, Hue is configured to run on port `8888` on the master node in a Dataproc cluster. To connect to the Hue web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. In the opened web browser go to 'localhost:8888' and you should see the Hue UI.

## Important notes

* If you wish to use Oozie in HUE, it must be installed before running this initialization action e.g. put the [Oozie initialization action](../oozie/README.md) before this one in the list to cloud.
