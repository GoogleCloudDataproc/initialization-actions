# Kafka 

This initialization action installs [Kafka](http://kafka.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Kafka depends on Zookeeper, which can either be installed with a separate zookeeper initialization action or by simply creating a "High Availability" cluster using `--num-masters 3`.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Kafka installed by:

1. Uploading a copy of this initialization action (`kafka.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you may need to increase the `initialization-action-timeout` if this script takes a long time to run. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --num-masters 3 \
        --image-version preview \
        --initialization-actions gs://dataproc-initialization-actions/kafka/kafka.sh
    ```
1. Once the cluster has been created Kafka should be running on all worker nodes in the cluster, and Kafka libraries should be installed on the master node(s). You can test your Kafka setup by creating a simple topic and publishing to it with Kafka's command-line tools, after SSH'ing into one of your master nodes:

    ```bash
    gcloud compute ssh <CLUSTER_NAME>-m-0

    # Create a test topic, just talking to the local master's zookeeper server.
    /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --replication-factor 1 --partitions 1 --topic test 
    /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

    # Use worker 0 as broker to publish 100 messages over 100 seconds
    # asynchronously.
    CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
    for i in {0..100}; do echo "message${i}"; sleep 1; done | \
        /usr/lib/kafka/bin/kafka-console-producer.sh \
        --broker-list ${CLUSTER_NAME}-w-0:9092 --topic test &

    # User worker 1 as broker to consume those 100 messages as they come.
    # This can also be run in any other master or worker node of the cluster.
    /usr/lib/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
        --topic test --from-beginning
    ```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script will install Kafka on all **worker nodes** by default (but not the master(s))
* This script assumes you have zookeeper installed, either through the zookeeper.sh init action or by creating a high-availability cluster with `--num-masters 3`
* The `delete.topic.enable` property has been set to `true` by default so topics can be deleted
* As of May 2017, this init action is only targeted to work on Dataproc's `--image-version preview` version and any subsequent numbered image versions derived from this preview. If you have a strong dependency on Kafka on Dataproc 1.1 or older, please reach out to dataproc-feedback@google.com
