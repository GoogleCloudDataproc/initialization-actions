from dask.distributed import Client
import dask.array as da
import dask_ml
import dask_bigquery

import numpy as np

client = Client("localhost:8786")

x = da.sum(np.ones(5))
x.compute()
