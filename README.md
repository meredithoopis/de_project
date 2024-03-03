Beginner data engineering project
===============================

This is a simple, for-beginner project for learning to orchestrate a data pipeline. \
Tech stack used: Spark, Kafka, Deltalake,Trino, Airflow, Postgres. \
Data can be downloaded at this link: [Link](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### Requirements
- Python 3.9.12(Other python versions are yet to be tested)
- Docker + Docker Compose 

## How-to
Clone this repository  and install the required packages: 
```bash 
pip install -r requirements.txt
```

Run docker-compose to start the data lake service: 
```bash
docker compose up -d
```

1. Push data to Minio: 
You can either access [localhost:9001](https://localhost:9001) to upload files or push the data manually to Minio by running the following command: 
```bash
python utils/upload_folder.py
```
![minio.png](imgs%2Fminio.png)

2. Access the trino container to create a database:
```bash
docker exec -it datalake-trino bash
```
After that, run the following in the terminal: 
```bash 
CREATE SCHEMA IF NOT EXISTS lakehouse.taxi
WITH (location = 's3://taxi/');

CREATE TABLE IF NOT EXISTS lakehouse.taxi.taxi (
  VendorID VARCHAR(50),
  tpep_pickup_datetime VARCHAR (50),
  tpep_dropoff_datetime VARCHAR (50),
  passenger_count DECIMAL,
  trip_distance DECIMAL,
  RatecodeID DECIMAL, 
  store_and_fwd_flag VARCHAR(50), 
  PULocationID VARCHAR(50),
  DOLocationID VARCHAR(50), 
  payment_type VARCHAR(50), 
  fare_amount DECIMAL, 
  extra DECIMAL, 
  mta_tax DECIMAL, 
  tip_amount DECIMAL, 
  tolls_amount DECIMAL, 
  improvement_surcharge VARCHAR(50),
  total_amount DECIMAL,
  congestion_surcharge DECIMAL, 
  Airport_fee DECIMAL
) WITH (
  location = 's3://taxi/part0'
);
```
3. Check Kafka service by going through [localhost:9021](localhost:9021)

![kafka.png](imgs%2Fkafka.png)

4. Check Airflow service, run the following commands: 
```bash
cd pipeline
```
and 
```
docker compose up -d
```





