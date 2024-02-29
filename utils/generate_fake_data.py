import pandas as pd
import numpy as np
import os
from helpers import load_config
import shutil
from deltalake.writer import write_deltalake

cfg_path = "/home/meredith/apps/cap/utils/config.yaml"
if __name__ == "__main__": 
    start_ts = '26-09-2022'
    end_ts = '25-09-2023'

    # Features to generate
    features = ['pressure', 'velocity', 'speed']
    df = pd.read_parquet("/home/meredith/apps/cap/utils/output.parquet")
    cfg = load_config(cfg_path)
    fake_data_cfg = cfg["fake_data"]
    num_files = fake_data_cfg['num_files']
    df_sampled = df.sample(frac=1).reset_index(drop=True)
    df_splits = np.array_split(df_sampled, num_files)
    for i in range(num_files):
        print(f"Processing file {i}...")
        if os.path.exists(            
            os.path.join(
                fake_data_cfg["folder_path"],
                f"part_{i}"
            )
        ):
            shutil.rmtree(            
                os.path.join(
                    fake_data_cfg["folder_path"],
                    f"part_{i}"
                )
            )
        write_deltalake(
            os.path.join(
                fake_data_cfg["folder_path"],
                f"part_{i}"
            ),
            df_splits[i].reset_index()
        )
        print(f"Generated the file {i} successfully!")
