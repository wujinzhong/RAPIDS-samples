import os
import numpy as np
import pandas as pd
import cudf
import dask_cudf
from dask.distributed import Client

client = Client("127.0.0.1:8487")
print(client.list_datasets())
shared_ddf = client.get_dataset('shared_dataset1')
print(shared_ddf)

