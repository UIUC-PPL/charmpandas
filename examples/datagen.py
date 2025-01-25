import numpy as np
import pandas as pd
from random import shuffle, randint

n = 1000000

first_names = np.array(["A%i" % i for i in range(n)])
last_names = np.array(["B%i" % i for i in range(n)])
user_ids = list(range(n))
ages = [i % 100 for i in range(n)]
city = ["C%i" % randint(0, 10000) for i in range(n)]

table1 = pd.DataFrame({"first_name" : first_names, "last_name" : last_names,
                       "user_id" : user_ids, "city" : city})

shuffle(user_ids)
first_names = first_names[user_ids]
last_names = last_names[user_ids]

table2 = pd.DataFrame({"first_name" : first_names, "last_name" : last_names,
                       "age" : ages})

table1.to_parquet("user_ids_large.parquet")
table2.to_parquet("ages_large.parquet")
