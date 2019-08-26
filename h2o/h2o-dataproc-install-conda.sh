#!/bin/bash

echo "BEGIN Stage 1 : Install H2O libraries and dependencies"

apt-get update

cd /usr/lib/
wget http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/13/sparkling-water-2.4.13.zip
unzip -o sparkling-water-2.4.13.zip

/opt/conda/default/bin/conda install tabulate future colorama scikit-learn
/opt/conda/default/bin/conda install --override-channels -c main -c conda-forge google-cloud-bigquery google-cloud-storage
/opt/conda/default/bin/conda install -c h2oai h2o h2o_pysparkling_2.4

echo "END Stage 1 : Install H2O libraries and dependencies"
