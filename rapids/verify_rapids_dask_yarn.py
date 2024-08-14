from dask.distributed import Client
import dask.array as da
import numpy as np

def test_dask_yarn():
  try:
    from dask_yarn import YarnCluster
  except:
    return

  # Validate dask_yarn configuration
  cluster = YarnCluster()
  client = Client(cluster)

  cluster.scale(4)
  x = da.sum(np.ones(5))
  x.compute()

test_dask_yarn() # known to fail for recent relases of rapids
