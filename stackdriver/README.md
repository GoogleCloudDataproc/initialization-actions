# Stackdriver Initialization Action

This initialization action downloads and installs the [Google Stackdriver](https://cloud.google.com/stackdriver/)
installations script. This will for monitoring a Cloud Dataproc cluster within Stackdriver. With this monitoring
you can, for example, look at fine-grained resource use across the cluster, alarm on various triggers, or
analyze the performance of your cluster.

## Using this initialization action

**You need to configure Stackdriver before you use this initialization action.** Specifically, you must create a group
based on the cluster name prefix of your cluster. Once you do, Stackdriver will detect any new instances created
with that prefix and use this group as the basis for your alerting policies and dashboards. You can create a new
group through the [Stackdriver user interface](https://app.google.stackdriver.com/groups/create).

Once you have configured a copy of this script, you can use this initialization action to create a new Dataproc cluster
with the Stackdriver agent installed by:

1. Uploading a copy of the initialization action (`stackdriver.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>` and specify the initialization action stored in `<GCS_BUCKET>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/stackdriver.sh
    ```
1. Once the cluster is online, Stackdriver should automatically start capturing data from your cluster. You can visit
the [Stackdriver interface](https://app.google.stackdriver.com/) to view the metrics.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* If you do not create a group in Stackdriver with the same prefix as your cluster name, Stackdriver will not
automatically pick up data from your cluster.
* Ensure you have reviewed the [pricing for Stackdriver](https://cloudplatform.googleblog.com/2016/06/announcing-pricing-for-Google-Stackdriver.html)
