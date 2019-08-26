#!/bin/bash

echo "BEGIN Stage 1 : Install H2O libraries and dependencies"

apt-get update
apt-get install -y python-dev python-pip jq

cd /usr/lib/
wget https://s3.amazonaws.com/h2o-release/sparkling-water/spark-2.4/3.26.2-2.4/sparkling-water-3.26.2-2.4.zip
unzip -o sparkling-water-3.26.2-2.4.zip

pip install requests tabulate future colorama scikit-learn google-cloud-bigquery google-cloud-storage
pip install -U https://h2o-release.s3.amazonaws.com/h2o/rel-yau/2/Python/h2o-3.26.0.2-py2.py3-none-any.whl  h2o_pysparkling_2.4


echo "END Stage 1 : Install H2O libraries and dependencies"
