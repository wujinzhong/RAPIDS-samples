import cudf

#gdf = cudf.DataFrame()
#gdf["event_day"]    = ["day0","day0","day1","day2"]
#gdf["target_url"]   = ["url0","url0","url1","url2"]
#gdf["userid"]       = ["uid0","uid0","uid1","uid2"]
#gdf["planid"]       = ["pid0","pid0","pid1","pid2"]
#gdf["unitid"]       = ["tid0","tid0","tid1","tid2"]
#gdf["cmatch"]       = ["cmt0","cmt0","cmt1","cmt2"]
#gdf["intt"]         = [11,10,11,12]
#gdf.to_csv("/thor/samples.csv")

gdf = cudf.read_csv("../../samples.csv")
print(gdf)


gdf_drop = gdf_query.drop_duplicates( ["event_day", "userid", "planid", "unitid"] )
print(gdf_drop)

#import nvstrings
#s = nvstrings.to_device(["day0","day0","day1","day2"])
#hash_s = s.hash()
#print("s:")
#print(s)
#print("hash_s:")
#print(hash_s)

