Beginner data engineering project
===============================

This is a simple, for-beginner project for learning to orchestrate a data pipeline. Tech stack used: Spark, Kafka, Deltalake,Trino, Airflow, Postgres.  

### Requirements
- Python 3.9.12
- Docker + Docker Compose 

### How-to
Clone this repository and run: 
```bash 
cd scr
```
Install required packages: 
```bash 
pip install -r requirements.txt
```
Running Docker to start all services, go to the corresponding port to check Spark master: 
```bash
docker-compose up -d
```

1. You can use the already crawled data or run the following command to crawl new data: 

```bash
python crawl.py
```

2. Go to the spark-master terminal to stream data through Socket: 

```bash
docker exec -it spark-master /bin/bash
```
After that, run the following: 
```bash 
cd jobs 
```
and 
```
python streaming-socket.py 
```

4. Aggregating data using Spark and streamed to Kafa: Open another terminal and run:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 jobs/spark-streaming.py
```

<details>

<summary>Illustrations</summary>

### Checking the topic in Kafka 
![confluent.png](imgs%2Fconfluent.png)

### Checking the indice in Elasticsearch cloud
![elastic.png](imgs%2Felastic.png)
</details>



