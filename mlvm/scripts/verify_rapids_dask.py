import cudf
import xgboost

# confirm can actually use RAPIDS libraries
df = cudf.DataFrame()
df['a'] = [0, 1, 2]
df['b'] = [1, 2, 3]
df['c'] = df.a * df.b + 100
dmat = xgboost.DMatrix(df)
