import cudf
import dask_cudf
import xgboost

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

test_rapids()
