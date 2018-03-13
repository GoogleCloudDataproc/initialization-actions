# RStudio Server

This initialization action installs the latest version of [RStudio Server](https://www.rstudio.com/products/rstudio/#Server) on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with RStudio Server installed by:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions \
            gs://dataproc-initialization-actions/rstudio/rstudio.sh
    ```
1. Once the cluster has been created, RStudio Server is configured to run on port `8787` on the master node in a Dataproc cluster. To connect to the Rtudio Server web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes & Known issues
* This installs RStudio Server 1.1.422
* Initial login is set to rstudio / rstudio
