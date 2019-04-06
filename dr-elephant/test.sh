#!/usr/bin/env bash
# This script is used for testing dr-elephant on 1.1,1.2,1.3 dataproc images with single,
# standard and HA configurations. After clusters are created, script submit spark jobs on them.
# Dr Elephant UI can be accessed after connection with command:
# gcloud compute ssh ${cluster_prefix}-*config*-${suffix}-m* -- -L 8080:${cluster_prefix}-*config*-${suffix}-m*:8080
# Then just open a browser and type localhost:8080 address.
set -x
gsutil cp dr-elephant.sh ${bucket_name}/dr-elephant/
cluster_prefix=$1
bucket_name=$2
# 1.1
image='1.1-deb9'
suffix='1-1'
gcloud dataproc clusters create ${cluster_prefix}-single-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --single-node \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-standard-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-ha-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m\
  --num-masters 3 --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

# 1.2
image='1.2-deb9'
suffix='1-2'
gcloud dataproc clusters create ${cluster_prefix}-single-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --single-node \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-standard-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-ha-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m\
  --num-masters 3 --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

# 1.3
image='1.3-deb9'
suffix='1-3'
gcloud dataproc clusters create ${cluster_prefix}-single-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --single-node \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-standard-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m \
  --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

gcloud dataproc clusters create ${cluster_prefix}-ha-${suffix} --subnet default \
  --zone us-central1-a --initialization-action-timeout 25m\
  --num-masters 3 --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
  --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
  --image-version ${image} \
  --initialization-actions "${bucket_name}/dr-elephant/dr-elephant.sh" &

#single
suffix='1-1'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-single-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-2'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-single-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-3'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-single-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &

#standard
suffix='1-1'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-standard-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-2'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-standard-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-3'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-standard-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &

#ha
suffix='1-1'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-ha-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-2'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-ha-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000 &
suffix='1-3'
gcloud dataproc jobs submit spark --cluster ${cluster_prefix}-ha-${suffix} \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
  -- 20000