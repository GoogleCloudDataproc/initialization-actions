# Ops Agent

With [Dataproc 2.2 image version](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2), we recommend installing [Google Cloud Ops Agent](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent) to obtain system metrics. 

This initialization action will install Ops Agent on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster and provide similar metrics as the [`--metric-sources=monitoring-agent-defaults`](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#monitoring_agent_metrics) setting which was supported until Dataproc 2.1.
[This page](https://cloud.google.com/monitoring/api/metrics_agent#oagent-vs-magent) highlights differences in metric collection between Ops Agent and legacy monitoring agent.

We provide two variants of this initialization action:
- `opsagent.sh` installs Ops Agent. [By default](https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent/configuration#default), it collects syslogs and system (node) metrics.
- `opsagent_nosyslog.sh` installs Ops Agent and also specifies a user configuration in order to skip syslogs collection from your cluster nodes. If this user configuration is not specified, Ops Agent will collect syslogs besides the system (node) metrics. You can further customize this configuration to collect logs and metrics from other third-party applications.

If you are looking for backward compatibility w.r.t. Dataproc image versions (2.1 and prior) that did not ingest syslogs from Dataproc clusters and want to retain the same behavior, please use `opsagent_nosyslog.sh`.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

## Install Ops Agent with default configuration

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.2 \
    --region=${REGION} \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent.sh
```

## Install Ops Agent collecting system metrics only (no syslogs)

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.2 \
    --region=${REGION} \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/opsagent/opsagent_nosyslog.sh
```
