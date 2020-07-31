# Apache Pulsar Initialization Action

Here we've got an initialization action that automates the installation of a single-cluster pulsar instance on bare metal. More detail can be found in the Pulsar documentation here: https://pulsar.apache.org/docs/en/deploy-bare-metal/

## Usage

Specify the .sh script's location when using gcloud to create your dataproc cluster. Pulsar requires Zookeeper, which can be installed by either including it as an optional component (https://cloud.google.com/dataproc/docs/concepts/components/zookeeper), or by creating a HA cluster by specifying --num-masters=3.

e.g.
```bash
gcloud dataproc clusters create cluster-name \
    --region=region \
    --initialization-actions=gs://init_action_storage/pulsar-initialization-action.sh \
    --optional-components=ZOOKEEPER \
    --metadata=pulsar-functions-enabled=true,builtin-connectors-enabled=true,tiered-storage-offloaders-enabled=true... \
    ... other flags ...
```

The initialization action also supports the optional installation of builtin connectors and/or tiered storage offloaders, and enabling pulsar functions. To do so, specify appropriate boolean values in the cluster's metadata (https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata).

e.g.
```bash
gcloud dataproc clusters create cluster-name \
    --region=region \
    --metadata=pulsar-functions-enabled=true,builtin-connectors-enabled=true,tiered-storage-offloaders-enabled=true... \
    ... other flags ...
```

The cluster can be tested by publishing a message using pulsar-client. 

Run from publisher
```bash
bin/pulsar-client produce \
  persistent://public/default/test \
  -n 1 \
  -m "Scooby Doo"
```

run from subscriber
```bash
bin/pulsar-client consume \
  persistent://public/default/test \
  -n 100 \
  -s "consumer-test" \
  -t "Exclusive"
```

