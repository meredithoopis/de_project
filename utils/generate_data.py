from deltalake.writer import write_deltalake 
import pandas as pd 
import os 
import pyarrow.parquet as pq

#PyArrow can be used to transform data between different formats and data types.
parquet_dir = "/home/dslab/code/Hn/test/utils/output.parquet"

#New parquet files with Delta Lake logs 
output_dir = "home/dslab/code/Hn/test/utils/data/taxi_combined"

if not os.path.exists(output_dir): 
    os.makedirs(output_dir)

for i, df in enumerate(parquet_dir): 
    partition_dir = os.path.join(output_dir, f"part{i}")
    write_deltalake(partition_dir, df)
