import dask, dask_cudf, socket
from dask.distributed import Client

import cuml, dask_cuml
import cugraph

import xgboost, dask_xgboost

if __name__ == "__main__":
    # connect to dask cluster
    client = Client(socket.gethostname()+':8786')

    # confirm at least 1 GPU worker
    assert(len(client.scheduler_info()['workers']) > 0)

    # confirm can actually use RAPIDS libraries
    import cudf
    df = cudf.DataFrame()
    df['a'] = [0, 1, 2]
    df['b'] = [1, 2, 3]
    df['c'] = df.a * df.b + 100
    dmat = xgboost.DMatrix(df)
