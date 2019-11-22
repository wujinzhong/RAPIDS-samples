import os
import cudf
import dask_cudf as ddf
import numpy as np
#from numba import cuda

fc_ad_wise = cudf.DataFrame()
fc_ad_wise["query"]        = cudf.Series(["que0","que0","que1","que2"])
fc_ad_wise["event_day"]    = cudf.Series(["20191101","20191102","20191103","20191101"])
fc_ad_wise["show"]         = cudf.Series([56,57,55,54], dtype=np.int32)
fc_ad_wise["click"]        = cudf.Series([69,68,67,66], dtype=np.int32)
fc_ad_wise["charge"]       = cudf.Series([71,79,72,73], dtype=np.int32)
#fc_ad_wise.to_csv("/thor/fc_ad_wise.csv")
#fc_ad_wise = cudf.read_csv("../../fc_ad_wise.csv")
print(fc_ad_wise)

if True:
    hash_query = fc_ad_wise.hash_columns(["query"])
    fc_ad_wise["hash_query"] = hash_query
    # fc_ad_wise = fc_ad_wise.drop(["query"])
    # print(hash_query)

    fc_ad_wise["event_day"]  = fc_ad_wise["event_day"].astype("int64")
    #fc_ad_wise["hash_event_day"] = hash_event_day
    #fc_ad_wise = fc_ad_wise.drop(["event_day"])
    # print(hash_event_day)
    print(fc_ad_wise["event_day"].dtype)

    #print(fc_ad_wise)
    str = "hash_query in [-1046842473]"
    fc_ad_wise = fc_ad_wise.query(str)
    print(fc_ad_wise)
fc_ad_wise = ddf.from_cudf(fc_ad_wise, npartitions=2)

ala10query_1573629174000 = cudf.DataFrame()
ala10query_1573629174000["query"]        = cudf.Series(["que0","que2","que3","que4","que5","que6","que7","que8"])
ala10query_1573629174000["event_day"]    = cudf.Series(["20191101","20191102","20191103","20191101","20191101","20191102","20191103","20191101"])
#ala10query_1573629174000["userid"]       = cudf.Series(["uid0","uid0","uid2","uid3"])
#ala10query_1573629174000.to_csv("/thor/ala10query_1573629174000.csv")
#ala10query_1573629174000 = cudf.read_csv("../../ala10query_1573629174000.csv")
#print(ala10query_1573629174000)

if True:
    hash_query = ala10query_1573629174000.hash_columns(["query"])
    ala10query_1573629174000["hash_query"] = hash_query
    # ala10query_1573629174000 = ala10query_1573629174000.drop(["query"])

    ala10query_1573629174000["event_day"] = ala10query_1573629174000["event_day"].astype("int64")
    #ala10query_1573629174000["hash_event_day"] = hash_event_day
    #ala10query_1573629174000 = ala10query_1573629174000.drop(["event_day"])
    # print(hash_event_day)
    print(ala10query_1573629174000["event_day"].dtype)

    str = "hash_query in [-1046842473]"
    ala10query_1573629174000 = ala10query_1573629174000.query(str)
    print(ala10query_1573629174000)
ala10query_1573629174000 = ddf.from_cudf(ala10query_1573629174000, npartitions=2)

#fc_ad_wise["hash_userid"] = [1,2,3,4]
#ala10query_1573629174000["hash_userid"] = [1,1,3]
#print(fc_ad_wise)
#print(ala10query_1573629174000)

join_table = fc_ad_wise.merge( ala10query_1573629174000, on=["hash_query"], how="left").compute()
print(join_table)

#join_table = join_table.drop(["userid_l", "userid_r"])
#print(join_table)

grouped_join_table = join_table.groupby(["hash_query"], method="cudf", as_index=False )
print(grouped_join_table)

aggs = {
        'show': ['sum'],
        'click': ['mean'],
        'charge': ['min'],
    }
grouped_aggs = grouped_join_table.agg(aggs)
print(grouped_aggs)

groups_df = grouped_join_table.as_df()
print(groups_df[0])
print(groups_df[1])

print(grouped_aggs)
#groups_df_aggs = grouped_aggs.as_df()
#print(groups_df_aggs[0])
#print(groups_df_aggs[1])

sorted_df = grouped_aggs.sort_values(by=["click"], ascending=True)
print(sorted_df)

