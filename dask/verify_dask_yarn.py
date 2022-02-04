from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.array as da
import dask_ml
import dask_bigquery

import numpy as np

cluster = YarnCluster()
client = Client(cluster)

x = da.sum(np.ones(5))
x.compute()
