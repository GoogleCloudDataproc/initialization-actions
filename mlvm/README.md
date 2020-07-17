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
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-machine-type n1-standard-16 \
        --worker-machine-type n1-highmem-32 \
        --worker-accelerator type=nvidia-tesla-t4,count=2 \
        --image-version preview-ubuntu \
        --metadata spark-bigquery-connector-version=0.13.1-beta \
        --metadata gpu-driver-provider=NVIDIA \
        --metadata rapids-runtime=SPARK \
        --metadata include-gpus=true \
        --optional-components ANACONDA,JUPYTER \
        --initialization-actions gs://dataproc-initialization-actions/mlvm/mlvm.sh \
        --enable-component-gateway  
    ```


You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).