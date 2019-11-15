import cudf
import numpy as np
from numba import cuda

fc_ad_wise = cudf.DataFrame()
fc_ad_wise["event_day"]    = cudf.Series(["20190708","20190709","20190710","20190708"])
fc_ad_wise["userid"]       = cudf.Series(["uid0","uid0","uid1","uid2"])
fc_ad_wise["term"]         = cudf.Series(["ter0","ter0","ter1","ter2"])
fc_ad_wise["target_url"]   = cudf.Series(["tgt0","tgt0","tgt1","tgt2"])
fc_ad_wise["charge"]       = cudf.Series([71,79,72,73], dtype=np.int32)
#fc_ad_wise.to_csv("/thor/fc_ad_wise.csv")
#fc_ad_wise = cudf.read_csv("../../fc_ad_wise.csv")
#print(fc_ad_wise)

if True:
    hash_event_day  = fc_ad_wise.hash_columns(["event_day"])
    #print(hash_event_day)
    hash_userid  = fc_ad_wise.hash_columns(["userid"])
    #print(hash_userid)

    fc_ad_wise["hash_event_day"] = hash_event_day
    fc_ad_wise["hash_userid"] = hash_userid
    #fc_ad_wise = fc_ad_wise.drop(["event_day", "userid"])
    #print(fc_ad_wise)

    #print(fc_ad_wise)
    str = "hash_event_day in [-1046842473,-1708607005,-555109064]"
    fc_ad_wise = fc_ad_wise.query(str)
    print(fc_ad_wise)

fc_advertisers = cudf.DataFrame()
fc_advertisers["event_day"]    = cudf.Series(["20190708","20190708","20190709","20190708"])
fc_advertisers["userid"]       = cudf.Series(["uid0","uid0","uid2","uid3"])
#fc_advertisers.to_csv("/thor/fc_advertisers.csv")
#fc_advertisers = cudf.read_csv("../../fc_advertisers.csv")
#print(fc_advertisers)

if True:
    hash_event_day = fc_advertisers.hash_columns(["event_day"])
    #print(hash_event_day)
    hash_userid = fc_advertisers.hash_columns(["userid"])
    #print(hash_userid)

    fc_advertisers["hash_event_day"] = hash_event_day
    fc_advertisers["hash_userid"] = hash_userid
    #fc_advertisers = fc_advertisers.drop(["event_day", "userid"])
    #print(fc_advertisers)

    print(fc_advertisers)
    str = "hash_event_day in [-1046842473]"
    fc_advertisers = fc_advertisers.query(str)
    print(fc_advertisers)

fc_ad_wise["hash_userid"] = [1,2,3,4]
fc_advertisers["hash_userid"] = [1,1,3]
print(fc_ad_wise)
print(fc_advertisers)

join_table = fc_ad_wise.join( fc_advertisers, on=["hash_userid"], how="left", lsuffix="_l", rsuffix="_r", method="hash", sort=True )
print(join_table)

join_table = join_table.drop(["userid_l", "userid_r"])
print(join_table)

grouped_join_table = join_table.groupby( ["hash_userid_r", "hash_event_day_r"], method="cudf" )
print(grouped_join_table)

groups_df = grouped_join_table.as_df()
print(groups_df[0])
print(groups_df[1])

#import nvstrings
#s = nvstrings.to_device(["world", "hello", "i think this is weired","hi","hello", "world"])
#s_hash = s.hash()
#print(s_hash)
