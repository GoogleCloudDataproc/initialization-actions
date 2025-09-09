# Ops Agent

With [Dataproc 2.2 image version](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2), we recommend installing [Google Cloud Ops Agent](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent) to obtain system metrics. 

This initialization action will install the Ops Agent on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster and provide similar metrics as the [`--metric-sources=monitoring-agent-defaults`](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#monitoring_agent_metrics) setting which was supported until Dataproc 2.1.
[This page](https://cloud.google.com/monitoring/api/metrics_agent#oagent-vs-magent) highlights differences in metric collection between the Ops Agent and the legacy monitoring agent.

We provide two variants of this initialization action:
- `opsagent.sh` installs the Ops Agent. [By default](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent/configuration#default), it collects syslogs and system (node) metrics.
- `opsagent_nosyslog.sh` installs the Ops Agent and also specifies a user configuration in order to skip syslogs collection from your cluster nodes. If the user configuration is not specified, Ops Agent will collect syslogs besides the system (node) metrics. You can further customize this configuration to collect logs and metrics from other third-party applications.

⚠️ **Dataproc clusters now have cluster-level syslog collection enabled by default.**  Starting **August 18, 2025,** new Dataproc clusters will have the property `dataproc.logging.syslog.enabled` set to `true`. This new default behavior can lead to log duplication if the Ops Agent is also configured to collect syslogs.

To prevent duplicate logs, we recommend using `opsagent_nosyslog.sh`. If you need to disable cluster-level syslog collection entirely, you can set the `dataproc.logging.syslog.enabled` property to `false` during cluster creation. For more details, refer to the [Dataproc Release Notes](https://cloud.google.com/dataproc/docs/release-notes#July_15_2025) and [Dataproc Logs documentation](https://cloud.google.com/dataproc/docs/guides/logging#cluster-logs).

If you are looking to match the behavior of Dataproc image versions up to 2.1 with `--metric-sources=monitoring-agent-defaults` without ingesting syslogs, please use opsagent_nosyslog.sh and additionally set the `dataproc.logging.syslog.enabled` property to `false` during cluster creation.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

## Install the Ops Agent collecting system metrics only (no syslogs)

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.2 \
    --region=${REGION} \
    --properties dataproc:dataproc.logging.syslog.enabled=false \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent_nosyslog.sh
```

## Install the Ops Agent with default configuration (Not Recommended  )
This approach is not recommended from August 18, 2025 as the cluster-level syslog collection is enabled by default for newly created clusters.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.2 \
    --region=${REGION} \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent.sh
```
