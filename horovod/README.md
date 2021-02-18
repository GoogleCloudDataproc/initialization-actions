# Horovod

[Horovod](horovod.ai) is an open source framework designed to distribute deep
learning jobs across multiple machines. You can distribute your own models or
take advantage of a series of Estimators provided by Horovod. Horovod is able to
run on top of Spark, allowing you to scale easily with features such as Dataproc
autoscaling.

This initialization action will set up Horovod on a Google Cloud Dataproc
cluster. You may configure Horovod to distribute its workloads using either MPI,
Gloo, or NCCL with GPUS.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

### Creating Dataproc Cluster with Horovod

The following command will create a Google Cloud Dataproc cluster with Horovod
installed.

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type n1-standard-16 \
  --worker-machine-type n1-standard-16 \
  --image-version 2.0-ubuntu18 \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/horovod/horovod.sh \
  --initialization-action-timeout=60m
```

By default, this will install Horovod with the `gloo` controller. You can use
the flag `--metadata use_mpi=true` to install OpenMPI and configure Horovod to
use it. Building MPI adds around 20 extra minutes to cluster creation. See
[here](https://horovod.readthedocs.io/en/stable/install_include.html#controllers)
for more information on the difference between the two controllers.

You can also use GPUs with Horovod and the Spark runner. You can create a
cluster with GPUs with the following command:

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type n1-standard-16 \
  --worker-machine-type n1-standard-16 \
  --master-accelerator nvidia-tesla-v100 \
  --worker-accelerator nvidia-telsa-v100 \
  --image-version 2.0-ubuntu18 \
  --metadata gpu-provider=NVIDIA \
  --metadata cuda-version=11.0 \
  --metadata cudnn-version=8.0.5.39 \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/horovod/horovod.sh \
  --initialization-action-timeout=60m
```

Note: Configuring Horovod to use MPI on a Dataproc cluster with GPUs is currently unsupported.

If you wish to use Horovod with GPU support without Spark, you must provide the
metadata flag `horovod-env-vars="HOROVOD_GPU_OPERATIONS=NCCL"`.

### Using Horovod

There are two ways to take advantage of Horovod's ability to scale out Deep
Learning jobs. The first is by using `horovod.spark.run`, which enables you to
scale out your own Deep Learning jobs:

```python
import horovod.spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def train(magic_number):
  import horovod.tensorflow as hvd
  hvd.init()

  # Your TensorFlow code...

horovod.spark.run(train)
```

Horovod with Spark also provides a series of
[estimators](https://horovod.readthedocs.io/en/stable/spark_include.html#horovod-spark-estimators)
for training Keras models on Spark Dataframes.

```python
from tensorflow import keras
import tensorflow as tf
import horovod.spark.keras as hvd

# Spark Dataframes
train_df = ...
test_df = ...

model = keras.models.Sequential() \
    .add(keras.layers.Dense(8, input_dim=2)) \
    .add(keras.layers.Activation('tanh')) \
    .add(keras.layers.Dense(1)) \
    .add(keras.layers.Activation('sigmoid'))

# NOTE: unscaled learning rate
optimizer = keras.optimizers.SGD(lr=0.1)
loss = 'binary_crossentropy'

store = HDFSStore('/user/username/experiments')
keras_estimator = hvd.KerasEstimator(
    num_proc=4,
    store=store,
    model=model,
    optimizer=optimizer,
    loss=loss,
    feature_cols=['features'],
    label_cols=['y'],
    batch_size=32,
    epochs=10)

keras_model = keras_estimator.fit(train_df) \
    .setOutputCols(['predict'])
predict_df = keras_model.transform(test_df)
```

Check out the official Horovod repository for
[end-to-end examples](https://github.com/horovod/horovod/tree/master/examples/spark/keras).

## Supported Libraries

This initialization action will build Horovod based on the following package
versions:

```bash
horovod==0.21.0
tensorflow=2.4.0
torch==1.7.1
torchvision==0.8.2
mxnet==1.7.0.post0 # CPUs only
```

This initialization action can also be configured with GPUs and the appropriate
libraries. Mote: MXNet is not installed if GPUs are present.

## Supported Metadata Parameters

This initialization action supports a series of metadata fields. The following
allow you to configure a specific version of one of the libraries shown above.
It is generally recommended to avoid doing this, as the versions above have been
carefully selected to be compatible with each other.

*   horovod-version=VERSION
*   tensorflow-version=VERSION
*   torch-version=VERSION
*   torchvision-version=VERSION
*   mxnet-version=VERSION
*   cuda-version=VERSION

You may also change the controller from the default of `gloo` to `mpi` via the
following flag. Please note this will increase setup time by about 20 minutes:

*   install-mpi=true

Additionally, you may also provide any Horovod
[environment variables](https://horovod.readthedocs.io/en/stable/install_include.html#environment-variables)
via a space-separated value.

*   horovod-env-vars="HOROVOD_ENV_VAR2=VAL2 HOROVOD_ENV_VAR2=VAL2"
