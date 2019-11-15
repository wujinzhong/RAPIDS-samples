import os
import numpy as np
import pandas as pd
import cudf
import dask_cudf
from dask.distributed import Client

np.random.seed(12)
df = cudf.DataFrame([('a', list(range(20))),
    ('b', list(reversed(range(20)))),
    ('c', list(range(20)))])
print(df)

ddf = dask_cudf.from_cudf(df, npartitions=2) 
print(ddf.persist())

client = Client("192.168.99.3:8487")
client.publish_dataset(shared_dataset1=ddf)
print(client.list_datasets())

