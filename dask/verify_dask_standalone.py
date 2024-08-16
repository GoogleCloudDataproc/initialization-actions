from dask.distributed import Client
import dask.array as da
import sys
import dask_ml
import dask_bigquery

import numpy as np

print("imports processed")

master_hostname="localhost"
if len(sys.argv) > 1:
  master_hostname=sys.argv[1]

print("Master node hostname: " + master_hostname)
print("timeout here means scheduler service may not be running")
client = Client(master_hostname+":8786")

print("Client instantiated")

x = da.sum(np.ones(5))

print("Simple dask array defined")
print("timeout here means worker service may not be running")
x.compute()

print("Simple dask array computed")

# https://examples.dask.org/array.html
x = da.random.random((10000, 10000), chunks=(1000, 1000))

print("Multi-dimension dask array defined")
x.compute()

print("Multi-dimension dask array computed")

y = x + x.T
z = y[::2, 5000:].mean(axis=1)

print("mean analysis defined")

z.compute()

print("mean analysis computed")
