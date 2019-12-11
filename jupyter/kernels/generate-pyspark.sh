#!/bin/bash
#
# Prints a generated JSON spec in to stdout which can be used to configure a
# pyspark Jupyter kernel, based on various version settings like the installed
# spark version.

set -e

SPARK_MAJOR_VERSION=$(spark-submit --version |&
  grep 'version' | head -n 1 | sed 's/.*version //' | cut -d '.' -f 1)
echo "Determined SPARK_MAJOR_VERSION to be '${SPARK_MAJOR_VERSION}'" >&2

# This will let us exit with error code if not found.
PY4J_ZIP=$(ls /usr/lib/spark/python/lib/py4j-*.zip)

# In case there are multiple py4j versions or unversioned symlinks to the
# versioned file, just choose the first one to use for jupyter.
PY4J_ZIP=$(echo ${PY4J_ZIP} | cut -d ' ' -f 1)
echo "Found PY4J_ZIP: '${PY4J_ZIP}'" >&2

if (("${SPARK_MAJOR_VERSION}" >= 2)); then
  PACKAGES_ARG=''
else
  PACKAGES_ARG='--packages com.databricks:spark-csv_2.10:1.3.0'
fi

cat <<EOF
{
 "argv": [
    "python", "-m", "ipykernel", "-f", "{connection_file}"],
 "display_name": "PySpark",
 "language": "python",
 "env": {
    "SPARK_HOME": "/usr/lib/spark/",
    "PYTHONPATH": "/usr/lib/spark/python/:${PY4J_ZIP}",
    "PYTHONSTARTUP": "/usr/lib/spark/python/pyspark/shell.py",
    "PYSPARK_SUBMIT_ARGS": "--master yarn --deploy-mode client ${PACKAGES_ARG} pyspark-shell"
 }
}
EOF
