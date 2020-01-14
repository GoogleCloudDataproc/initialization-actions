# HUE - Hadoop User Experience

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs the latest version of [Hue](http://gethue.com) on a master node within
a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Hue
installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hue/hue.sh
    ```

1.  Once the cluster has been created, Hue is configured to run on port `8888`
    on the master node in a Dataproc cluster. To connect to the Hue web
    interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy
    with your web browser as described in the
    [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
    documentation. In the opened web browser go to 'localhost:8888' and you
    should see the Hue UI.

## Important notes

*   If you wish to use Oozie in Hue, it must be installed before running this
    initialization action e.g. put the
    [Oozie initialization action](../oozie/README.md) before this one in the
    list to cloud.
