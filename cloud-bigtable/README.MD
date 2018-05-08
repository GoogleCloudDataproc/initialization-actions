# Google Cloud Bigtable via Apache HBase
This initialization action installs Apache HBase libraries and the [Google Cloud Bigtable](https://cloud.google.com/bigtable/) [HBase Client](https://github.com/GoogleCloudPlatform/cloud-bigtable-client).


## Using this initialization action
You can use this initialization action to create a Dataproc cluster configured to connect to Cloud Bigtable:

1. Create a Bigtable instance by following [these directions](https://cloud.google.com/bigtable/docs/creating-instance).
1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/bigtable.sh \
    --metadata bigtable-instance=<BIGTABLE INSTANCE>
    ```
1. When it is finished being created the cluster will have HBase and the Bigtable Client on both Spark and Hadoop classpaths.
1. You can validate the deployment by SSHing to the master (`gcloud compute ssh <CLUSTER_NAME?-m`) and running `hbase shell`.

## Important notes
* You can edit and upload your own copy of `bigtable.sh` to Google Cloud Storage and use that instead.
* Specifying the Bigtable instance is optional, but if you do not specify it you will need to set `google.bigtable.instance.id` in your Hadoop Configurations (e.g. as a job property), and the HBase shell will not function.
* If you wish to use an instance in another project you can specify `--metadata bigtable-project=<PROJECT>` (this will set `google.bigtable.project.id`). Make sure your cluster's service account is authorized to access the instance.
* If you specify custom service account scopes, make sure to add [appropriate Bigtable scopes](https://cloud.google.com/bigtable/docs/creating-compute-instance#choosing_title_short_scopes) or `cloud-platform`. Clusters have `bigtable.admin.table` and `bigtable.data`, by default.
