#!/bin/bash
#
# Prints a generated JSON spec in to stdout which can be used to configure a
# pyspark Jupyter kernel, based on various version settings like the installed
# spark version.

set -e

REPO_URL="https://dist.apache.org/repos/dist"
/opt/conda/bin/pip \
    install \
    ${REPO_URL}/release/incubator/toree/0.2.0-incubating/toree-pip/toree-0.2.0.tar.gz

SPARK_MAJOR_VERSION=$(spark-submit --version |& \
  grep 'version' | head -n 1 | sed 's/.*version //' | cut -d '.' -f 1)
echo "Determined SPARK_MAJOR_VERSION to be '${SPARK_MAJOR_VERSION}'" >&2

# This will let us exit with error code if not found.
PY4J_ZIP=$(ls /usr/lib/spark/python/lib/py4j-*.zip)

# In case there are multiple py4j versions or unversioned symlinks to the
# versioned file, just choose the first one to use for jupyter.
PY4J_ZIP=$(echo ${PY4J_ZIP} | cut -d ' ' -f 1)
echo "Found PY4J_ZIP: '${PY4J_ZIP}'" >&2

COMMON_PACKAGES='org.vegas-viz:vegas_2.11:0.3.11,org.vegas-viz:vegas-spark_2.11:0.3.11'
if (( "${SPARK_MAJOR_VERSION}" >= 2 )); then
  PACKAGES_ARG="--packages ${COMMON_PACKAGES}"
else
  PACKAGES_ARG="--packages com.databricks:spark-csv_2.10:1.3.0,${COMMON_PACKAGES}"
fi

SPARK_OPTS="--master yarn --deploy-mode client ${PACKAGES_ARG}"
/opt/conda/bin/jupyter toree install \
    --spark_opts="${SPARK_OPTS}" \
    --spark_home="/usr/lib/spark" \
    --kernel_name="Toree"
