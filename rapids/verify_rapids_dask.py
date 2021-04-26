import cudf
import dask_cudf
import xgboost

from dask.distributed import Client
import dask.array as da
import numpy as np

def test_rapids():
    # confirm RAPIDS and xgboost are available
    df = cudf.DataFrame()
    df['a'] = [0, 1, 2]
    df['b'] = [1, 2, 3]
    df['c'] = df.a * df.b + 100
    dmat = xgboost.DMatrix(df)

    # confirm Dask is available
    ds = dask_cudf.from_cudf(df['c'], npartitions=2)
    ds.compute()

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

test_rapids()
test_dask_yarn()
