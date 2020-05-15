# Machine Learning VM

This initialization action installs a set of packages designed to get you up and running with many commonly-used machine learning packages on a
[Dataproc](https://cloud.google.com/dataproc) cluster.

Features of this configuration include:

* Python packages such as  TensorFlow, PyTorch, MxNet, Scikit-learn and Keras
* R packages such as XGBoost, Caret, randomForest, sparklyr
* GCS, Hadoop-BigQuery and Spark-BigQuery Connectors
* RAPIDS on Spark and GPU support
* Jupyter and Anaconda via [Optional Components](https://cloud.google.com/dataproc/docs/concepts/components/overview)
* [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)


## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
a set of preconfigured machine learning packages.:

1.  Use the `gcloud` command to create a new cluster with this initialization action. The command shown below includes a curated list of packages that will be installed on the cluster:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
        gcloud beta dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-machine-type n1-standard-8 \
        --worker-machine-type n1-highmem-32 \
        --worker-accelerator type=nvidia-tesla-t4,count=2 \
        --image-version 1.5-ubuntu \
        --metadata gcs-connector-version=2.1.1 \
        --metadata bigquery-connector-version=1.1.1 \
        --metadata spark-bigquery-connector-version=0.13.1-beta \
        --metadata PIP_PACKAGES="google-cloud-bigquery google-cloud-datalabeling google-cloud-storage google-cloud-bigtable google-cloud-dataproc google-api-python-client mxnet tensorflow numpy rapidsai scikit-learn keras spark-nlp xgboost torch torchvision" \
        --metadata gpu-driver-provider=NVIDIA \
        --metadata rapids-runtime=SPARK \
        --scopes "cloud-platform" \
        --optional-components ANACONDA,JUPYTER \
        --properties "spark:spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_2.x-1.0.0-Beta4.jar" \
        --initialization-actions gs://dataproc-initialization-actions/ml-vm/ml-vm.sh \
        --subnet=default \
        --enable-component-gateway  
    ```

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).