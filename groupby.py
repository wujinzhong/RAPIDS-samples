import cudf
import numpy as np
from numba import cuda

gdf = cudf.DataFrame()
gdf["event_day"]    = cudf.Series(["day0","day0","day1","day2"])
gdf["cmatch"]       = cudf.Series([21,21,22,23], dtype=np.int32)
gdf["query"]        = cudf.Series(["qur0","qur0","qur1","qur2"])
gdf["pv"]           = cudf.Series([31,39,32,33], dtype=np.int32)
gdf["epv"]          = cudf.Series([41,49,42,43], dtype=np.int32)
gdf["show"]         = cudf.Series([51,59,52,53], dtype=np.int32)
gdf["click"]        = cudf.Series([61,69,62,63], dtype=np.int32)
gdf["charge"]       = cudf.Series([71,79,72,73], dtype=np.int32)
#gdf.to_csv("/thor/samples.csv")
#gdf = cudf.read_csv("../../samples.csv")
print(gdf)

hash_event_day  = gdf.hash_columns(["event_day"])
#print(hash_event_day)
hash_query  = gdf.hash_columns(["query"])
#print(hash_query)
#hash_encode = gdf["event_day"].hash_encode(10)
#print(hash_encode)
#hash_values = gdf["event_day"].hash_values()
#print(hash_values)

gdf["hash_event_day"]   = hash_event_day
gdf["hash_query"]       = hash_query
gdf = gdf.drop(["event_day", "query"])
print(gdf)

#gdf2 = cudf.DataFrame()
#gdf2.add_column("hash_event_day", gdf.hash_event_day)
#gdf2.add_column("cmatch",gdf.cmatch)
#gdf2.add_column("hash_query",gdf.hash_query)
#print(gdf2)

#gdf2["hash_event_day"] = [0, 0, 1, 1, 2, 2, 2]
#gdf2["hash_query"] = [0, 1, 2, 3, 4, 5, 6]
#gdf2["cmatch"] = [7, 8, 9, 10, 11, 12, 13]
#print(gdf2)

expr = "(hash_event_day == -1046842473 and cmatch == 21)"
gdf_query = gdf.query(expr)
#gdf_query = gdf2
print(gdf_query)

groups = gdf_query.groupby( ["hash_event_day"], method="cudf" )
print("groups: \n", groups)

groups_mean = gdf_query.groupby( ["hash_event_day"], method="cudf" ).mean()
print("groups mean: \n", groups_mean)

groups_max = gdf_query.groupby( ["hash_event_day"], method="cudf" ).max()
print("groups max: \n", groups_max)

aggs = {
        'click': ['mean'],
        'show': ['min'],
        'epv': ['max', 'mean'],
        'charge': ['sum'],
    }
groups_agg = gdf_query.groupby( ["hash_event_day"], method="cudf" ).agg(aggs)
print("groups agg: \n", groups_agg)

def mult_add(hash_event_day, cmatch, out0, out1):
    #for i in range(cuda.threadIdx.x, len(key), cuda.blockDim.x):
    i = cuda.threadIdx.x
    out0[i] = hash_event_day[i] * cmatch[i]
    out1[i] = hash_event_day[i] + cmatch[i]

result = groups.apply_grouped(mult_add,
                              incols=["hash_event_day", "cmatch"],
                              outcols={'out0': np.int32, 'out1': np.int32}
                            )
print(result)

groups_df = groups.as_df()
#print(groups_df)
#print(groups_df[0])
print(groups_df[1])

gmean = groups.mean()
print(gmean)



#gmean = groups.value.mean()
#print(gmean)

#def sum(df):
#    df['out'] = df['hash_event_day'] + df['cmatch']
#    return df

#result = groups.apply(sum)
#print(result)

#import nvstrings
#s = nvstrings.to_device(["day0","day0","day1","day2"])
#hash_s = s.hash()
#print("s:")
#print(s)
#print("hash_s:")
#print(hash_s)
