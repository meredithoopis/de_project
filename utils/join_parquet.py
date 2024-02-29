import pandas as pd
import pyarrow.parquet as pq

if __name__ == "__main__": 
    parquet_file_paths = [
        "/home/meredith/apps/cap/utils/data/yellow_tripdata_2022-08.parquet", 
        "/home/meredith/apps/cap/utils/data/yellow_tripdata_2022-09.parquet"
]
    with pq.ParquetWriter("output.parquet", schema = pq.ParquetFile(parquet_file_paths[0]).schema_arrow) as writer: 
        for file in parquet_file_paths: 
            writer.write_table(pq.read_table(file))