#!/usr/bin/env bash

gsutil cp hue.sh ${bucket_name}/hue
cluster_prefix=$1
bucket_name=$2
# 1.3
image='1.3'
suffix='13'
gcloud dataproc clusters create test-ha-1 --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m\
  --num-masters 3 --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version 1.2 --project polidea-dataproc-testing \
  --initialization-actions "gs://polidea-dataproc-utils/hue/hue.sh"
