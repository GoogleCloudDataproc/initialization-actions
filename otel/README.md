# OpenTelemetry

[OpenTelemetry](https://opentelemetry.io/docs/) is a collection of APIs, SDKs, and tools. Use it to instrument, generate, collect, and export telemetry data (metrics, logs, and traces) to help you analyze your software’s performance and behavior.

This initialization action installs OpenTelemetry Collector Contrib ([otelcol-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)) in a Dataproc cluster, configures OTel and 
pulls metrics from Prometheus endpoints provided by the customer. The metrics are then exported to Google Cloud Monitoring (GCM). 

## Using the initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
OTel installed on every node:

- Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    PROMETHEUS_ENDPOINTS=<prometheus_endpoints>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --metadata=prometheus-scrape-endpoints="${PROMETHEUS_ENDPOINTS}"
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/otel/otel.sh
    ```
 - Use the `master-only` metadata property to install the agent only on master nodes.

    ```bash
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --metadata=prometheus-scrape-endpoints="${PROMETHEUS_ENDPOINTS}",master-only="true"
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/otel/otel.sh
    ```
NOTE: Add a string of space separated prometheus scrape urls in `PROMETHEUS_ENDPOINTS`, example value: `0.0.0.0:1234 0.0.0.0:1235`

---

### Usage warning
This initialisation action is provided without support and currently does not have an integration test. Read more [here](https://github.com/GoogleCloudDataproc/initialization-actions/#why-these-samples-are-provided).