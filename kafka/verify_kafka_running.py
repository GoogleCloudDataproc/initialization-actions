"""verify_kafka_running.py: Script for Kafka initialization action test.
"""
import os
import sys
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC_NAME = 'test'
NUM_MESSAGES = 10
WORKER_0_NODE_SUFFIX = '-w-0'
WORKER_1_NODE_SUFFIX = '-w-1'
PORT = '9092'
NUM_RETRIES = 3
SLEEP_TIME_SECS = 1
DATAPROC_IMAGE_VERSION = os.environ['DATAPROC_IMAGE_VERSION']


def get_cluster_name():
    """Gets full cluster name.

    Returns:
      cluster_name: full name of the cluster
    """
    command = ('/usr/share/google/get_metadata_value '
               'attributes/dataproc-cluster-name')
    with os.popen(command) as process:
        return process.read()


class KafkaTestRunner(object):
    """Interfaces with Apache Kafka python client and command-line tools.

    Args:
      cluster_name (str): full cluster name of test cluster
    """

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name

    def _get_address(self, node_suffix, port):
        """Generates full address from cluster name, suffix and port.

        Args:
          node_suffix (str): node suffix
          port (str): connection port for worker node

        Returns:
          string: address specified by node_suffix and port
        """
        return self.cluster_name + node_suffix + ':' + port

    def create_test_topic(self):
        """Creates topic.

        Raises:
          Exception: if topic creation is unsuccessful
        """
        if DATAPROC_IMAGE_VERSION >= "2.1":
            args = ('--bootstrap-server localhost:9092 --create --replication-factor 1 '
                    '--partitions 1 --topic %s') % TOPIC_NAME
        else:
            args = ('--zookeeper localhost:2181 --create --replication-factor 1 '
                    '--partitions 1 --topic %s') % TOPIC_NAME
        command = '/usr/lib/kafka/bin/kafka-topics.sh ' + args
        if os.WEXITSTATUS(os.system(command)) != os.EX_OK:
            raise Exception('Create test topic unsuccessful')

    def publish_messages(self):
        """Publishes messages using worker 0 as the broker.

           Fails if publishing is unsuccessful.

        Raises:
          Exception: if publishing results in KafkaError
        """
        node_name = self._get_address(WORKER_0_NODE_SUFFIX, PORT)
        print('Publishing ' + node_name)

        producer = KafkaProducer(
            bootstrap_servers=[node_name],
            retries=NUM_RETRIES)

        try:
            for i in range(NUM_MESSAGES):
                time.sleep(SLEEP_TIME_SECS)
                print(i)
                if DATAPROC_IMAGE_VERSION >= "2.1":
                    producer.send(TOPIC_NAME, bytes('message' + str(i), 'utf-8'))
                else:
                    producer.send(TOPIC_NAME, 'message' + str(i))
        except KafkaError:
            raise Exception('Publish messages unsuccessful')
        finally:
            producer.flush()
            producer.close()

    def consume_messages(self):
        """Consumes messages using worker 1 as the broker.

          Fails if consuming is unsuccessful or if number of published messages does
          not match the number of consumed messages.

        Raises:
          Exception: if consuming results in KafkaError, or mismatch in number of
            published/consumed messages
        """
        node_name = self._get_address(WORKER_1_NODE_SUFFIX, PORT)
        print('Consuming ' + node_name)

        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[node_name],
            auto_offset_reset='smallest',
            request_timeout_ms=120000)

        try:
            if DATAPROC_IMAGE_VERSION < "2.1":
                consumer_iter = iter(consumer)

                # ensures consumer receives all messages sent by producer
                for _ in range(NUM_MESSAGES):
                    message = consumer_iter.next()
                    print(message.value)
        except KafkaError:
            raise Exception('Consume messages unsuccessful')
        finally:
            consumer.close()


def parse_kafka_config():
    config = {}
    with open('/etc/kafka/conf/server.properties') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                (key, value) = line.split('=', 1)
                config[key] = value
    print(config)
    return config


def verify_kafka_config():
    config = parse_kafka_config()
    expected_logs_dirs = sys.argv[1]
    if config['log.dirs'] != expected_logs_dirs:
        raise Exception('Invalid log.dirs, expected: {}, actual: {}'.format(
            expected_logs_dirs, config['log.dirs']))


def test_produce_and_consume_messages():
    cluster_name = get_cluster_name()
    kafka_runner = KafkaTestRunner(cluster_name)
    kafka_runner.create_test_topic()
    kafka_runner.publish_messages()
    kafka_runner.consume_messages()


def main():
    verify_kafka_config()
    test_produce_and_consume_messages()


if __name__ == '__main__':
    main()
