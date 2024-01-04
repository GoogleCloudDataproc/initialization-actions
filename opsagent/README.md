# Ops Agent

With [Dataproc 2.2 image version](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2), we recommend installing [Google Cloud Ops Agent](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent) to obtain system metrics. 

This initialization action will install Ops Agent on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster and provide similar metrics as the [`--metric-sources=monitoring-agent-defaults`](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#monitoring_agent_metrics) setting which was supported until Dataproc 2.1.
[This page](https://cloud.google.com/monitoring/api/metrics_agent#oagent-vs-magent) highlights differences in metric collection between Ops Agent and legacy monitoring agent.

This initialization action also specifies a user configuration in order to skip syslogs collection from your cluster nodes. If this is not specified, Ops Agent will collect syslogs besides the system (node) metrics. You can further customize this configuration to collect logs and metrics from other third-party applications.

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
        --image-version=2.2 \
        --region=${REGION} \
        --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent.sh
    ```

