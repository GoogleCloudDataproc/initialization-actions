# Kafka

This initialization action installs [Kafka](http://kafka.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Kafka depends on Zookeeper, which can either be installed with [Zookeeper component](https://cloud.google.com/dataproc/docs/concepts/components/zookeeper) or by simply creating a "High Availability" cluster using `--num-masters 3` parameter.

By default, Kafka brokers run only on all worker nodes in the cluster, and Kafka libraries and command-line tools are installed on the master node(s) at `/usr/lib/kafka`. But you can specify to run Kafka broker(s) on master node(s) with `--metadata "run-on-master=true"`.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Kafka installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --num-masters 3 \
        --metadata "run-on-master=true" \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
    ```
1. You can test your Kafka setup by creating a simple topic and publishing to it with Kafka's command-line tools, after SSH'ing into one of your nodes:

    ```bash
    gcloud compute ssh <CLUSTER_NAME>-m-0

    # Create a test topic, just talking to the local master's zookeeper server.
    /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --replication-factor 1 --partitions 1 --topic test
    /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

    # Use worker 0 as broker to publish 100 messages over 100 seconds
    # asynchronously.
    CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
    for i in {0..100}; do echo "message${i}"; sleep 1; done |
        /usr/lib/kafka/bin/kafka-console-producer.sh \
            --broker-list ${CLUSTER_NAME}-w-0:9092 --topic test &

    # User worker 1 as broker to consume those 100 messages as they come.
    # This can also be run in any other master or worker node of the cluster.
    /usr/lib/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
        --topic test --from-beginning
    ```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Installing Kafka Manager with Kafka

If you would like to use [Kafka Manager](https://github.com/yahoo/kafka-manager) to manage your Kafka cluster through web UI, use the `gcloud` command to create a new Kafka cluster with the Kafka Manager initialization action. The following command will create a new high availability Kafka cluster with Kafka Manager running on the first master node. The default HTTP port for Kafka Manager is 9000. Follow the instructions at [Dataproc cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to access the web UI.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --num-masters 3 \
    --metadata "run-on-master=true" \
    --metadata "kafka-enable-jmx=true" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh,gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka-manager.sh
```

## Installing Cruise Control with Kafka

If you would like to use [Cruise Control](https://github.com/linkedin/cruise-control) to automate common Kafka operations, e.g., automatically fixing under-replicated partitions caused by broker failures, use the `gcloud` command to create a new Kafka cluster with the Cruise Control initialization action. The following command will create a new high availability Kafka cluster with Cruise Control running on the first master node. The default HTTP port for Cruise Control is 9090. Follow the instructions at [Dataproc cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to access the web UI.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --num-masters 3 \
    --metadata "run-on-master=true" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh,gs://goog-dataproc-initialization-actions-${REGION}/kafka/cruise-control.sh
```

## Installing Prometheus with Kafka

If you would like to use [Prometheus](https://github.com/prometheus/prometheus) to monitor your Kafka cluster, use the `gcloud` command to create a new Kafka cluster with the Prometheus initialization action. The following command will create a new high availability Kafka cluster with Prometheus server listening on port 9096 of each node. Follow the instructions at [Dataproc cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to access the web UI.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --num-masters 3 \
    --metadata "run-on-master=true" \
    --metadata "prometheus-http-port=9096" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh,gs://goog-dataproc-initialization-actions-${REGION}/prometheus/prometheus.sh
```

## Important notes

* This script will install Kafka on all **worker nodes** by default (but not the master(s))
* This script assumes you have zookeeper installed, either through the zookeeper.sh init action or by creating a high-availability cluster with `--num-masters 3`
* The `delete.topic.enable` property has been set to `true` by default so topics can be deleted
* This init action is only targeted to work on Dataproc's 1.2 version or later including preview version. If you have a strong dependency on Kafka on Dataproc 1.1 or older, please reach out to dataproc-feedback@google.com
