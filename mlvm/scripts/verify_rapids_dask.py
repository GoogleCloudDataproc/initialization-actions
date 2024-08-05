import pandas as pd
import xgboost as xgb

# Create a Pandas DataFrame
df = pd.DataFrame({
    'a': [0, 1, 2],
    'b': [1, 2, 3]
})
df['c'] = df['a'] * df['b'] + 100

dmat = xgb.DMatrix(df)

computed_df = df['c']
