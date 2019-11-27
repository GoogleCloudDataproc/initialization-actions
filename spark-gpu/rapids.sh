#!/bin/bash

set -euxo pipefail

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly LINUX_DIST=$(/usr/share/google/get_metadata_value attributes/linux-dist)

readonly DEAFULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEAFULT_INIT_ACTIONS_REPO})"

readonly DEAFULT_GCS_BUCKET=gs://my-bucket
readonly GCS_BUCKET="$(/usr/share/google/get_metadata_value attributes/GCS_BUCKET ||
  echo ${DEAFULT_GCS_BUCKET})"

readonly DEAFULT_RAPIDS_SPARK_VERSION=2.x-1.0.0-Beta3
readonly RAPIDS_SPARK_VERSION="$(/usr/share/google/get_metadata_value attributes/RAPIDS_SPARK_VERSION ||
  echo ${DEAFULT_RAPIDS_SPARK_VERSION})"

readonly DEAFULT_RAPIDS_CUDF_VERSION=0.9.2-cuda10
readonly RAPIDS_CUDF_VERSION="$(/usr/share/google/get_metadata_value attributes/RAPIDS_CUDF_VERSION ||
  echo ${DEAFULT_RAPIDS_CUDF_VERSION})"

echo "Cloning RAPIDS initialization action from '${INIT_ACTIONS_REPO}' ..."
RAPIDS_INIT_ACTION_DIR=$(mktemp -d -t rapids-init-action-XXXX)
readonly RAPIDS_INIT_ACTION_DIR
gsutil -m rsync -r "${INIT_ACTIONS_REPO}/spark-gpu" "${RAPIDS_INIT_ACTION_DIR}"

if [[ "${LINUX_DIST}" == 'ubuntu' ]]; then
  mv "${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver-ubuntu.sh" "${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver.sh"
else
  mv "${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver-debian.sh" "${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver.sh"
fi
find "${RAPIDS_INIT_ACTION_DIR}" -name '*.sh' -exec chmod +x {} \;

if [[ "${ROLE}" != 'Master' ]]; then
  # Ensure we have GPU drivers installed.
  "${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver.sh"
else
  gsutil cp ${GCS_BUCKET}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar /usr/lib/spark/python/lib/
  gsutil cp ${GCS_BUCKET}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar /usr/lib/spark/jars/
  gsutil cp ${GCS_BUCKET}/xgboost4j_${RAPIDS_SPARK_VERSION}.jar /usr/lib/spark/jars/
  gsutil cp ${GCS_BUCKET}/cudf-${RAPIDS_CUDF_VERSION}.jar /usr/lib/spark/jars/
fi



