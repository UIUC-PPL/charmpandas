import charmpandas as pd
from charmpandas.interface import CCSInterface

pd.set_interface(CCSInterface("100.115.92.204", 1234))

df = pd.read_parquet("/home/adityapb1546/charm/charmpandas/examples/userdata.parquet")

print(df.get())
