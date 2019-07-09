#!/bin/bash

git clone https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git
cd dataproc-initialization-actions
gsutil -m rsync -e -R -x '^.git/' . gs://dataproc-initialization-actions
cd ..
rm -rf dataproc-initialization-actions
