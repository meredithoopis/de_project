from deltalake import DeltaTable 
from deltalake.writer import write_deltalake 
import pandas as pd 
import numpy as np 
import datetime 

print("*" * 80)
table = DeltaTable("data/taxi-data/")
print("Current delta table: ")
print(table.to_pandas())

df = pd.DataFrame({
    'index': 1000, 
    'event_timestamp': [np.random.choice(
        pd.date_range(
            datetime.datetime(2023, 9, 26), 
            datetime.datetime(2023, 9, 27)
        )
    )], 
    'pressure': [np.random.rand()], 
    'velocity': [np.random.rand()], 
    'speed': [np.random.rand()]
})
print(df)

write_deltalake(table, df, mode="append")
print("Final table: ")
table1 = DeltaTable("data/taxi-data/")
print(table1.to_pandas())
