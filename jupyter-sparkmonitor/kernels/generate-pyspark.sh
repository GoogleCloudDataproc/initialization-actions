#!/bin/bash
#
# Prints a generated JSON spec in to stdout which can be used to configure a
# pyspark Jupyter kernel, based on various version settings like the installed
# spark version.

set -e

SPARK_MAJOR_VERSION=$(spark-submit --version |& \
  grep 'version' | head -n 1 | sed 's/.*version //' | cut -d '.' -f 1)
echo "Determined SPARK_MAJOR_VERSION to be '${SPARK_MAJOR_VERSION}'" >&2

readonly PYTHON_PATH=/opt/conda/anaconda/bin/python

if (( "${SPARK_MAJOR_VERSION}" >= 2 )); then
  PACKAGES_ARG=''
else
  PACKAGES_ARG='--packages com.databricks:spark-csv_2.10:1.3.0'
fi

cat << EOF
{
 "argv": [
    "python", "-m", "ipykernel", "-f", "{connection_file}"],
 "display_name": "PySpark",
 "language": "python",
 "env": {
    "SPARK_HOME": "/usr/lib/spark/",
    "PYTHONPATH": "${PYTHON_PATH}"
 }
}
EOF
