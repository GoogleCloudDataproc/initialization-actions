import pandas as pd
import dask.dataframe as dd

import dask.array as da
import numpy as np

def test_pandas_dask():
  # Create a simple pandas DataFrame
  df = pd.DataFrame()
  df['a'] = [0, 1, 2]
  df['b'] = [1, 2, 3]
  df['c'] = df.a * df.b + 100

  # Convert the pandas DataFrame to a Dask DataFrame
  ddf = dd.from_pandas(df['c'], npartitions=2)

  # Compute the Dask DataFrame
  computed_df = ddf.compute()

  # Print the results
  print("Pandas DataFrame:")
  print(df)
  print("\nComputed Dask DataFrame:")
  print(computed_df)

def test_dask_yarn():
  x = da.sum(np.ones(5))
  result = x.compute()

  print("\nDask-Yarn Result:")
  print(result)

# Run the test functions
test_pandas_dask()
test_dask_yarn()
