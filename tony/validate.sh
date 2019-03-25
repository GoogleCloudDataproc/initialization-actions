#!/usr/bin/env bash
# This script is used for testing TonY on 1.3 Dataproc images with standard configuration.
# After clusters is created, script submit a Hadoop jobs on them.
# Usage: You can easily test TonY is running by using ```validate.sh ${cluster_prefix} ${bucket_name}``` script.

set -x

readonly CLUSTER_PREFIX='dev'
readonly BUCKET_NAME='tony-dev'
readonly TONY_JARFILE='tony-cli-0.2.0-all.jar'

if [ -z "$1" ]
  then
   cluster_prefix="${CLUSTER_PREFIX}"
else
  cluster_prefix=$1
fi

if [ -z "$2" ]
  then
   bucket_name="${BUCKET_NAME}"
else
  bucket_name=$2
fi

# Copy script to Bucket.
gsutil cp tony.sh gs://${bucket_name}/tony/

# 1.3
image='1.3-deb9'
suffix='1-3'

# Create standard cluster
gcloud dataproc clusters create "${cluster_prefix}"-standard-"${suffix}" --bucket "${bucket_name}" \
  --subnet default --zone us-central1-a --initialization-action-timeout 25m \
  --master-machine-type n1-standard-4 --master-boot-disk-size 50 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 100 \
  --image-version ${image} \
  --initialization-actions gs://"${bucket_name}/tony/tony.sh"

echo 'Launching TensorFlow job'
gcloud dataproc jobs submit hadoop --cluster "${cluster_prefix}-standard-${suffix}" \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars file:///opt/tony/TonY-samples/"${TONY_JARFILE}" -- \
--src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
--task_params='--data_dir /tmp/ --working_dir /tmp/' \
--conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
--executes mnist_distributed.py \
--python_venv=/opt/tony/TonY-samples/deps/tf.zip \
--python_binary_path=tf/bin/python3.5

echo 'Launching PyTorch job'
gcloud dataproc jobs submit hadoop --cluster "${cluster_prefix}-standard-${suffix}" \
--class com.linkedin.tony.cli.ClusterSubmitter \
--jars file:///opt/tony/TonY-samples/"${TONY_JARFILE}" -- \
--src_dir=/opt/tony/TonY-samples/jobs/PTJob/src \
--task_params='--root /tmp/' \
--conf_file=/opt/tony/TonY-samples/jobs/PTJob/tony.xml \
--executes mnist_distributed.py \
--python_venv=/opt/tony/TonY-samples/deps/pytorch.zip \
--python_binary_path=pytorch/bin/python3.5
