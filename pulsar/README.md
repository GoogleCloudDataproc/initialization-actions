# Apache Pulsar Initialization Action

This initialization action installs Apache Pulsar on Dataproc clusters. More detail can be found in the Pulsar documentation here: https://pulsar.apache.org/docs/en/deploy-bare-metal/

## Usage

Specify the .sh script's location when using gcloud to create your Dataproc cluster. Pulsar requires Zookeeper, which can be installed by either including it as an optional component (https://cloud.google.com/dataproc/docs/concepts/components/zookeeper), or by creating a HA cluster by specifying --num-masters=3. Pulsar version defaults to 2.6.0, but can be modified by supplying an appropriate value to pulsar-version in the gcloud's metadata field. 

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

The script will install the pulsar binary on every node in your cluster, regardless of role. It will install and configure bookies and brokers on worker nodes. Default port definitions follow pulsar best practice guidelines. Script logs can be found on-VM by navigating to /var/log and checking out dataproc-initialization-script-0.log. 


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

