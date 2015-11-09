# Apache Zeppelin

This initialization action installs the latest version of [Apache Zeppelin](https://zeppelin.incubator.apache.org/). To install this initialization action, set the `initialization-action-timeout` to at least 15 minutes, e.g.

```bash
gcloud beta dataproc clusters create <CLUSTER_NAME> --initialization-actions gs://<GCS BUCKET>/zeppelin.sh --initialization-action-timeout 15m
```

Zeppelin is configured to run on port `8080` on the master node in a Dataproc cluster. To connect to the Apache Zeppelin web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.
