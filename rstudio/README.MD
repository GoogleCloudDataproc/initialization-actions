# RStudio Server

This initialization action installs the Open Source Edition of [RStudio Server](https://www.rstudio.com/products/rstudio/#Server) on the master node of a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with RStudio Server installed by:

1. Using the `gcloud` command to create a new cluster with this initialization action. The password must be at least 7 characters.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/rstudio/rstudio.sh \
        --metadata rstudio-user=rstudio \
        --metadata rstudio-password=<PASSWORD>
    ```

1. Once the cluster has been created, RStudio Server is configured to run on port `8787` on the master node in a Dataproc cluster. To connect to the Rtudio Server web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation, then login with the user and password. Be careful about your firewall settings, not to expose port `8787` to the internet.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* RStudio is licensed under [AGPL v3](https://www.gnu.org/licenses/agpl-3.0-standalone.html)
* This installs RStudio Server
* Initial login is set to `rstudio` / `rstudio`
