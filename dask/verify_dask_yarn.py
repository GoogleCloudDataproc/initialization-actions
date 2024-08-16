from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.array as da
import dask_ml
import dask_bigquery
import skein

import numpy as np

print("imports successful")

print("configuring skein client")
skein.Client.stop_global_driver(force=True)
skein.Client.start_global_driver(log_level='debug', log='/tmp/verify_dask_yarn-driver.log')

print("skein version follows")
print(skein.__version__)

print("creating an application specification")
spec = skein.ApplicationSpec.from_yaml("""
name: verify-skein
queue: default

master:
  script: echo "Things worked!"
""")

print("Instantiating skein.Client object")
skeinClient = skein.Client.from_global_driver()

skeinClient.get_applications()
skeinClient.submit(spec)

print("Instantiating dask-yarn.YarnCluster object")
import time
for i in range(10):
    try:
       time.sleep(0.3)
       skeinClient = skein.Client.from_global_driver()
       cluster = YarnCluster(skein_client=skeinClient)
       break
    except Exception as err:
        print("retrying failed YarnCluster instantiation")
        time.sleep(5)

print("Instantiating dask.distributed.Client object")
client = Client(cluster)

print("Client object instantiated")
x = da.sum(np.ones(5))

print("created problem using dask.array.sum")
x.compute()

print("sum problem solved")
