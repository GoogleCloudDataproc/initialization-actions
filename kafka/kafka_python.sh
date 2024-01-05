#!/bin/bash

# Initialization action script that installs kafka-python on all nodes in
# a cluster and is used in the Kafka integration test (KafkaTest.java).
# Uploaded to gs://dataproc-kafka-test.

OS=$(. /etc/os-release && echo "${ID}")
if [[ "${OS}" == "rocky" ]]; then
  yum install -y python2-pip
else
  apt-get install -y python-pip
fi

KAFKA_PYTHON_PACKAGE="kafka-python==2.0.2"
pip2 install "${KAFKA_PYTHON_PACKAGE}" || { sleep 10; pip2 install "${KAFKA_PYTHON_PACKAGE}"; } || { sleep 10; pip install "${KAFKA_PYTHON_PACKAGE}"; }