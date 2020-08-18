from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.dataframe as da

import numpy as np

cluster = YarnCluster()
client = Client(cluster)

x = da.sum(np.ones(5))
x.compute()