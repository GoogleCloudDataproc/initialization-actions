from google.cloud import bigquery
from google.cloud import datalabeling
from google.cloud import storage
from google.cloud import bigtable
from google.cloud import dataproc
import google.api.python.client
import mxnet as mx
import tensorflow as tf
import numpy as np
import scikit.learn
import keras
import rpy2
import spark-nlp
import xgboost
import torch
import torchvision

"google-cloud-bigquery==1.24.0"
"google-cloud-datalabeling==0.4.0"
"google-cloud-storage==1.28.1"
"google-cloud-bigtable==1.2.1"
"google-cloud-dataproc==0.8.0"
"google-api-python-client==1.8.4"
"mxnet==1.6.0"
"tensorflow==2.2.0"
"numpy==1.18.4"
"scikit-learn==0.23.1"
"keras==2.3.1"
"rpy2==3.3.3"
"spark-nlp==2.5.1"
"xgboost==1.1.0"
"torch==1.5.0"
"torchvision==0.6.0"

# #mxnet
# from mxnet import nd
# from mxnet.gluon import nn


# layer = nn.Dense(2)
# layer

# layer.initialize()

# x = nd.random.uniform(-1, 1, (3, 4))
# layer(x)

# layer.weight.data()

# net = nn.Sequential()
# # Add a sequence of layers.
# net.add(  # Similar to Dense, it is not necessary to specify the input channels
#     # by the argument `in_channels`, which will be  automatically inferred
#     # in the first forward pass. Also, we apply a relu activation on the
#     # output. In addition, we can use a tuple to specify a  non-square
#     # kernel size, such as `kernel_size=(2,4)`
#     nn.Conv2D(channels=6, kernel_size=5, activation='relu'),
#     # One can also use a tuple to specify non-symmetric pool and stride sizes
#     nn.MaxPool2D(pool_size=2, strides=2),
#     nn.Conv2D(channels=16, kernel_size=3, activation='relu'),
#     nn.MaxPool2D(pool_size=2, strides=2),
#     # The dense layer will automatically reshape the 4-D output of last
#     # max pooling layer into the 2-D shape: (x.shape[0], x.size/x.shape[0])
#     nn.Dense(120, activation="relu"),
#     nn.Dense(84, activation="relu"),
#     nn.Dense(10))
# net
