import dask.array as da

import numpy as np

x = da.sum(np.ones(5))
x.compute()
