# Datalab Initialization Action

This initialization action downloads and runs a [Google Cloud Datalab](https://cloud.google.com/datalab/) Docker container on a Dataproc cluster. You will need to connect to Datalab using an SSH tunnel.

Once you have configured a copy of this script, you can use this initialization action to create a new Dataproc cluster with the Datalab server installed by:

1. Uploading a copy of the initialization action (`datalab.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>` and specify the initialization action stored in `<GCS_BUCKET>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://<GCS_BUCKET>/datalab/datalab.sh \
        --scopes cloud-platform
    ```
1. Once the cluster has been created, Datalab is configured to run on port `8080` on the master node in a Dataproc cluster. To connect to the Datalab web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.
1. Once you bring up a notebook, you should have the normal PySpark environment configured with `sc`, `sqlContext`, and `spark` predefined.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Notes

* PySpark's [`DataFrame.toPandas()`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.toPandas) method is useful for integrating with Datalab APIs.
  * Remember that Panda's DataFrames must fit on the master, whereas Spark's can fill a cluster.
  * Datalab has a number of notebooks documenting its [Pandas](http://pandas.pydata.org/)' integrations.
* This script requires that Datalab run on port `:8080`. If you normally run another server on that port (e.g. Zeppelin), consider moving it. Note running multiple Spark sessions can consume a lot of cluster resources and can cause problems on moderately small clusters.
* If you [build your own Datalab images](https://github.com/googledatalab/datalab/wiki/Development-Environment), you can specify `--metadata=docker-image=gcr.io/<PROJECT>/<IMAGE>` to point to your image.
* If you normally only run Datalab kernels on VMs and connect to them with a local Docker frontend, set the flag `--metadata=docker-image=gcr.io/cloud-datalab/datalab-gateway` and then set `GATEWAY_VM` to your cluster's master node in your local `docker`command [as described here](https://cloud.google.com/datalab/docs/quickstarts/quickstart-gce#install_the_datalab_docker_container_on_your_computer).
* You can pass Spark packages as a comma separated list with `--metadata spark-packages=<PACKAGES>` e.g. `--metadata '^#^spark-packages=com.databricks:spark-avro_2.11:3.2.0,graphframes:graphframes:0.3.0-spark2.0-s_2.11`.
