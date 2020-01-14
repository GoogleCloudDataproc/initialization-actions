# Stackdriver Initialization Action [ DEPRECATED ]

## Deprecation Note
As of December 10, 2018, Cloud Dataproc Stackdriver Monitoring is automatically enabled on Cloud Dataproc clusters, and collects and reports HDFS, YARN, and other Cloud Dataproc cluster and job metrics. See [Release Notes](https://cloud.google.com/dataproc/docs/release-notes#december_10_2018). This initialization action effectively does nothing for clusters created after this date.

This initialization action downloads and installs the [Google Stackdriver](https://cloud.google.com/stackdriver/)
installations script. This will enable monitoring for a Cloud Dataproc cluster within Stackdriver. With this monitoring
you can, for example, look at fine-grained resource use across the cluster, alarm on various triggers, or
analyze the performance of your cluster. 

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

**You need to configure Stackdriver before you use this initialization action.** Specifically, you must create a group
based on the cluster name prefix of your cluster. Once you do, Stackdriver will detect any new instances created
with that prefix and use this group as the basis for your alerting policies and dashboards. You can create a new
group through the [Stackdriver user interface](https://app.google.stackdriver.com/groups/create).

Once you have configured a copy of this script, you can use this initialization action to create a new Dataproc cluster
with the Stackdriver agent installed:

1. Use the `gcloud` command to create a new cluster with this initialization action. You must add the [requisite stackdriver monitoring scope(s)](https://cloud.google.com/monitoring/api/authentication#cloud_monitoring_scopes).

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/stackdriver/stackdriver.sh \
        --scopes https://www.googleapis.com/auth/monitoring.write
    ```
1. Once the cluster is online, Stackdriver should automatically start capturing data from your cluster. You can visit
the [Stackdriver interface](https://app.google.stackdriver.com/) to view the metrics.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Useful Tips

To better identify your cluster in a Stackdriver dashboard, you'll likely want to append a unique tag when creating
your cluster:

    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/stackdriver/stackdriver.sh \
        --scopes https://www.googleapis.com/auth/monitoring.write \
        --tags my-dataproc-cluster-20160901-1518

This way, even if you reuse your cluster in the future, you can easily disambiguate which incarnation
of the cluster you want to look at in your Stackdriver dashboards. For convenience, you may also want to use
to Google-hosted copy of the dataproc-initialization-actions repo; for example, once you've enabled the Stackdriver
APIs you can simply copy/paste:

    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/stackdriver/stackdriver.sh \
        --scopes https://www.googleapis.com/auth/monitoring.write \
        --tags ${USER}-dataproc-cluster-$(date +%Y%m%d-%H%M%S)

## Important notes

* If you do not create a group in Stackdriver with the same prefix as your cluster name or using the unique tag you
provided at cluster-creation time, Stackdriver will not automatically pick up data from your cluster.
* Ensure you have reviewed the [pricing for Stackdriver](https://cloudplatform.googleblog.com/2016/06/announcing-pricing-for-Google-Stackdriver.html)
