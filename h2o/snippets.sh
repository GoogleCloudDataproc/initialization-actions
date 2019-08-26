This Script installs H2O Sparkling Water ML Libraries on the Google Cloud Dataproc cluster.
It is inspired from H2O's documentation (http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/deployment/sw_google_cloud_dataproc.html)



https://github.com/conda/conda/issues/7690
https://github.com/conda/conda/issues/7690#issuecomment-451582942


gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/gobblin/gobblin.sh



gcloud dataproc clusters create some-name --region global --initialization-actions gs://h2o-dataproc/init-actions/h2o-dataproc-install-pip.sh --initialization-actions gs://h2o-dataproc/init-actions/h2o-dataproc-tune.sh


gcloud beta dataproc clusters create cluster-001 \
--enable-component-gateway \
--subnet default \
--zone us-central1-a \
--master-machine-type n1-standard-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4
--worker-boot-disk-size 500 \
--image-version 1.4-debian9 \
--optional-components ANACONDA \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--project tewariy-20181112 \
--initialization-actions 'gs://h2o-dataproc/init-actions/h2o-dataproc-install-pip.sh','gs://h2o-dataproc/init-actions/h2o-dataproc-tune.sh'




gcloud beta dataproc clusters create cluster-001 \
--enable-component-gateway \
--subnet default \
--zone us-central1-a \
--image-version 1.4-debian9 \
--optional-components ANACONDA \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--initialization-actions 'gs://h2o-dataproc/init-actions/h2o-dataproc-install-pip.sh','gs://h2o-dataproc/init-actions/h2o-dataproc-tune.sh'




gcloud beta dataproc clusters create cluster-003 \
--enable-component-gateway \
--subnet default \
--zone us-central1-a \
--image-version 1.4-debian9 \
--optional-components ANACONDA \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--initialization-actions 'gs://h2o-dataproc/init-actions/h2o-dataproc-install-conda.sh','gs://h2o-dataproc/init-actions/h2o-dataproc-tune.sh'


gcloud beta dataproc clusters create cluster-004 \
--enable-component-gateway \
--subnet default \
--zone us-central1-a \
--image-version 1.4-debian9 \
--optional-components ANACONDA \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--initialization-actions 'gs://h2o-dataproc/init-actions/h2o-dataproc-install-python_pip.sh','gs://h2o-dataproc/init-actions/h2o-dataproc-tune.sh'
