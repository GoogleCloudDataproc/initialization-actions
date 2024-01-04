# Ops Agent

With Dataproc 2.2, we recommend installing [Google Cloud Ops Agent](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent) to obtain system metrics.

This initialization action will install Ops Agent on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster and provide similar metrics as the [monitoring-agent-defaults option](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#monitoring_agent_metrics) (supported until Dataproc 2.1).

This initialization action also specifies a user configuration in order to skip syslogs collection from your cluster nodes. If this is not specified, Ops Agent will collect syslogs besides the system (host) metrics.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

- Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent.sh
    ```

