import pandas as pd
import dask.dataframe as dd
import xgboost as xgb

# Confirm XGBoost is available
# Create a Pandas DataFrame
df = pd.DataFrame({
    'a': [0, 1, 2],
    'b': [1, 2, 3]
})
df['c'] = df['a'] * df['b'] + 100

# Convert the DataFrame to a DMatrix for XGBoost
dmat = xgb.DMatrix(df)

# Confirm Dask is available
# Convert the DataFrame to a Dask DataFrame
ds = dd.from_pandas(df['c'], npartitions=2)

# Compute the Dask DataFrame
computed_ds = ds.compute()