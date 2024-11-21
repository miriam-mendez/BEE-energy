from dedl_stack_client.dask import DaskMultiCluster
import time
from distributed import Client
from xgboost import dask as dxgb
import dask.dataframe as dd
import dask.array as da
import dask.distributed

myAuth = DESPAuth()

# Connect Dask
myDEDLClusters = DaskMultiCluster(auth=myAuth)
myDEDLClusters.new_cluster()
time.sleep(10)
client = Client('tls://10.100.4.15:8786', security={"ssl_context": ssl_context})