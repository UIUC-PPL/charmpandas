import charmpandas as pd
from charmpandas.interface import LocalCluster

cluster = LocalCluster()

pd.set_interface(cluster)

df1 = pd.read_parquet("/home/adityapb1546/charm/charmpandas/examples/test1.parquet")
df2 = pd.read_parquet("/home/adityapb1546/charm/charmpandas/examples/test2.parquet")

#df3 = pd.concat([df1, df2])

#df3 = df1.groupby(["first_name", "last_name"])["ids"].count()

#df4 = df2.join(df3, ["first_name", "last_name"])

df1["test"] = df1["ids"] + 2 * df2["age"]
df2["test"] = 2. * df1["test"]

df3 = df2[df2["test"] > 100]

df3.print()

#print(x)
