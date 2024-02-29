from minio import Minio 
from helpers import load_config 
from glob import glob 
import os 

cfg_file = "/home/meredith/apps/cap/utils/config.yaml"
def main(): 
    config = load_config(cfg_file)
    datalake_cfg = config['datalake']
    client = Minio(
        endpoint = datalake_cfg['endpoint'], 
        access_key=datalake_cfg['access_key'], 
        secret_key=datalake_cfg['secret_key'],
        secure=False
    )
    #Create bucket if not exist 
    found = client.bucket_exists(bucket_name=datalake_cfg['bucket_name'])
    if not found: 
        client.make_bucket(bucket_name=datalake_cfg['bucket_name'])
    else: 
        print(f"Bucket {datalake_cfg['bucket_name']} already exists")
    
    parquet_files = glob(os.path.join("/home/meredith/apps/cap/data/taxi-data", "*.parquet"))
    json_files = glob(os.path.join("/home/meredith/apps/cap/data/taxi-data", "**", "*.json"), recursive=True)
    for file in parquet_files: 
        print(f"Uploading {file}")
        client.fput_object(
            bucket_name=datalake_cfg['bucket_name'], 
            object_name= os.path.join(
                datalake_cfg['folder_name'], os.path.basename(file)
            ), 
            file_path=file
        )
    
    for file in json_files: 
        print(f"Uploading {file}")
        client.fput_object(
            bucket_name=datalake_cfg['bucket_name'], 
            object_name= os.path.join(
                datalake_cfg['folder_name'], os.path.basename(file)
            ), 
            file_path=file
        )

if __name__ == "__main__": 
    main()