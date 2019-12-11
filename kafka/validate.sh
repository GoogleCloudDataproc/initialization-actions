#!/usr/bin/env bash

num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
hostname="$(hostname)"

echo "---------------------------------"
echo "Starting validation on ${hostname}"

# Create a test topic, just talking to the local master's zookeeper server.
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create \
  --replication-factor 1 --partitions 1 --topic ${hostname}
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

# Use worker 0 as broker to publish 100 messages over 100 seconds
# asynchronously.
echo "Testing worker-0 as producer and worker-1 as consumer"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
for i in {0..50}; do
  echo "message${i}"
  sleep 1
done |
  /usr/lib/kafka/bin/kafka-console-producer.sh \
    --broker-list ${CLUSTER_NAME}-w-0:9092 --topic ${hostname} &

# User worker 1 as broker to consume those 100 messages as they come.
# This can also be run in any other master or worker node of the cluster.
/usr/lib/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
  --topic ${hostname} --from-beginning >/tmp/messages &

received_messages=$(wc -l /tmp/messages | awk '{print $1}')
while [[ ${received_messages} -ne 51 ]]; do
  sleep 5
  received_messages=$(wc -l /tmp/messages | awk '{print $1}')
  echo "${received_messages} so far."
done

echo "Testing worker-0 as consumer and worker-1 as producer"
for i in {0..50}; do
  echo "message${i}"
  sleep 1
done |
  /usr/lib/kafka/bin/kafka-console-producer.sh \
    --broker-list ${CLUSTER_NAME}-w-1:9092 --topic ${hostname} &

/usr/lib/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
  --topic ${hostname} --from-beginning >/tmp/messages1 &

received_messages=$(wc -l /tmp/messages1 | awk '{print $1}')
while [[ ${received_messages} -ne 102 ]]; do
  sleep 5
  received_messages=$(wc -l /tmp/messages1 | awk '{print $1}')
  echo "${received_messages} so far."
done

echo ""
echo "Kafka test executed successfully."
echo ""

echo "---------------------------------"
