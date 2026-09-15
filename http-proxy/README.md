# HTTP Proxy Configuration

This initialization action configures global HTTP and HTTPS proxy settings on every node in a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. 

It is designed to set up proxy environments for clusters running in private networks that must egress through a secure web proxy or gateway.

## Features

- Configures global proxy environment variables (`http_proxy`, `https_proxy`, `no_proxy` and their uppercase variants) in `/etc/environment`.
- Persists proxy settings for all shell sessions via `/etc/profile.d/proxy.sh`.
- Bypasses proxying for Google Cloud APIs and internal GCP domains (e.g. `metadata.google.internal`, `169.254.169.254`, `.googleapis.com`, local cluster hostnames) using a robust default `no_proxy` list.
- Automatically appends custom bypass hosts from the `no-proxy` metadata.
- Configures the `gcloud` CLI proxy settings to align with the environment.
- Installs the proxy's PEM CA certificate (if provided) to the OS, Java, and Conda trust stores.
- Configures system package managers (`apt`/`dnf`) and `dirmngr` to fetch packages through the proxy.
- Configures `boto.cfg` (used by `gsutil`) to use the proxy.

## Parameters

You configure the proxy settings using VM metadata:

| Metadata Key | Description |
|---|---|
| `http-proxy` | (Optional) The HTTP proxy host and port (e.g. `10.0.0.1:8080` or `vzproxy.verizon.com:9290`). |
| `https-proxy` | (Optional) The HTTPS proxy host and port. |
| `proxy-uri` | (Optional) A unified proxy host and port if HTTP and HTTPS proxies are the same. Used as fallback if `http-proxy` or `https-proxy` are not set. |
| `no-proxy` | (Optional) A comma-separated list of additional hosts/domains that should bypass the proxy. |
| `http-proxy-pem-uri` | (Optional) A Cloud Storage URI (e.g. `gs://my-bucket/proxy_ca.crt`) containing the PEM-encoded CA certificate for the proxy. Required if the proxy inspects SSL traffic. |

## Usage

### ⚠️ CRITICAL COMPATIBILITY REQUIREMENT ⚠️

For Dataproc internal components (like HDFS NameNode) to successfully initialize and access the Google Cloud metadata server during boot, **this initialization action must run before system services start.**

You **must** set the following cluster property:
`dataproc:dataproc.master.custom.init.actions.mode=RUN_BEFORE_SERVICES`

### Example

Use the `gcloud` command to create a new cluster with this initialization action:

```bash
PROJECT_ID="my-project-id"
REGION="us-east4"
CLUSTER_NAME="my-proxy-cluster"
PROXY_HOST_PORT="vzproxy.verizon.com:9290"
CA_CERT_URI="gs://my-secure-bucket/proxy_ca.crt"

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://dataproc-initialization-actions-${REGION}/http-proxy/http-proxy.sh \
    --properties "dataproc:dataproc.master.custom.init.actions.mode=RUN_BEFORE_SERVICES" \
    --metadata "proxy-uri=${PROXY_HOST_PORT}" \
    --metadata "http-proxy-pem-uri=${CA_CERT_URI}" \
    --metadata "no-proxy=my-onprem-service.corp.internal"
```
