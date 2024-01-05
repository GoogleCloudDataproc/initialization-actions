#!/bin/bash

# Initialization action script that installs kafka-python on all nodes in
# a cluster and is used in the Kafka integration test (KafkaTest.java).
# Uploaded to gs://dataproc-kafka-test.

KAFKA_PYTHON_PACKAGE="kafka-python==2.0.2"

/opt/conda/default/bin/pip install "${KAFKA_PYTHON_PACKAGE}" || { sleep 10; /opt/conda/default/bin/pip install "${KAFKA_PYTHON_PACKAGE}"; }