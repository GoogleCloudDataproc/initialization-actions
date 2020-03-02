#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script installs and makes available the latest version of the 
# spark-bigquery-connector on all nodes of a Cloud Dataproc cluster.
# If a specific version of the connector is desired, you can select any 
# publicly available connector in gs://spark-lib/bigquery and add the 
# optional metadata key SPARK_BIGQUERY_CONNECTOR_VERSION to your cluster 
# creation command:
# 
# gcloud dataproc clisters create ${CLUSTER_NAME} \
#     --region ${REGION} \ 
#     --metadata 'SPARK_BIGQUERY_CONNECTOR_VERSION=spark-bigquery-latest_2.12.jar' \
#     --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/spark-bigquery-connector/install_connector.sh

echo "Installing spark-bigquery-connector"
readonly VERSION=$(/usr/share/google/get_metadata_value attributes/SPARK_BIGQUERY_CONNECTOR_VERSION || true)
readonly DEFAULT_VERSION=spark-bigquery-latest.jar

if [[ -z "${VERSION}"]]; then
  VERSION=${DEFAULT_VERSION}
fi

readonly SPARK_BIGQUERY_CONNECTOR_BUCKET=gs://spark-lib/bigquery/${VERSION}
readonly INSTALL_LOCATION=/opt/spark-bigquery-connector
readonly SPARK_BIGQUERY_PATH=${INSTALL_LOCATION}/${VERSION}

mkdir -p ${INSTALL_LOCATION}
gsutil cp "${SPARK_BIGQUERY_CONNECTOR_BUCKET}" "${SPARK_BIGQUERY_PATH}"

# Edit the spark-conf to enable Spark to be able to find the jar
if grep -q "spark.driver.extraClassPath" "${SPARK_CONF}"; then
  grep -q "spark.driver.extraClassPath" | sed -i "s/$/${SPARK_BIGQUERY_PATH}" "${SPARK_CONF}"
else
  echo -e "\n#Spark Driver Extra Jars stored here" >> "${SPARK_CONF}"
  echo -e "spark.driver.extraClassPath=${SPARK_BIGQUERY_PATH}" >> "${SPARK_CONF}"
fi

if grep -q "spark.executor.extraClassPath" "${SPARK_CONF}"; then
  grep -q "spark.executor.extraClassPath" | sed -i "s/$/${SPARK_BIGQUERY_PATH}" "${SPARK_CONF}"
else
  echo -e "\n#Spark Executor Extra Jars stored here" >> "${SPARK_CONF}"
  echo -e "spark.executor.extraClassPath=${SPARK_BIGQUERY_PATH}" >> "${SPARK_CONF}"
fi