import charmpandas as pd
from charmpandas.interface import CCSInterface

pd.set_interface(CCSInterface("100.115.92.204", 1234, odf=4))

df1 = pd.read_parquet("/home/adityapb1546/charm/charmpandas/examples/table1.parquet")
df2 = pd.read_parquet("/home/adityapb1546/charm/charmpandas/examples/table2.parquet")

#df3 = pd.concat([df1, df2])

df3 = df2.groupby(["first_name"])["ids"].sum()

df3.print()
