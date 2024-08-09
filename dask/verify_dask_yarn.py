from dask_yarn import YarnCluster
import dask.array as da

import numpy as np

cluster = YarnCluster()

x = da.sum(np.ones(5))
x.compute()
