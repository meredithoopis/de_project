import argparse 
import io 
import json 
from datetime import datetime 
from time import sleep 
import random 
import numpy as np 
from bson import json_util 
from kafka import KafkaAdminClient, KafkaProducer 
from kafka.admin import NewTopic 

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m", "--mode", default="setup", choices = ["setup", "teardown"], 
    help = "Setup or teardown a Kafka topic?"
)
parser.add_argument(
    "-b", "--bootstrap_servers", default="localhost:9092", help="The bootstrap server"
)
parser.add_argument(
    "-c", "--schemas_path", default="./avro_schemas", help="Location of schema file"
)

args = parser.parse_args()
NUM_TAXI = 1 

def create_topic(admin, topic_name): 
    try: 
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topic([topic])
        print(f"Created new topic: {topic_name}")
    except Exception: 
        print(f"Topic {topic_name} already exists")
        pass 

def create_streams(servers, schema_path): 
    producer = None 
    admin = None 
    for _ in range(10): 
        try: 
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("Initializing producer and admin success")
            break 
        except Exception as e: 
            print(f"Trying to reconnect: {servers}")
            sleep(10)
            pass 
    
    while True: 
        record = {
            "schema": {
            "type": "struct",
                "fields": [
                {
                    "type": "int64",
                    "optional": False,
                    "field": "VendorID"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "tpep_pickup_datetime"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "tpep_dropoff_datetime"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "passenger_count"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "trip_distance"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "RatecodeID"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "store_and_fwd_flag"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "PULocationID"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "DOLocationID"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "payment_type"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "fare_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "extra"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "mta_tax"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "tip_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "tolls_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "improvement_surcharge"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "total_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "congestion_surcharge"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "airport_fee"
                }
                ]
            }
        }
        record["payload"] = {}
        record["payload"]["taxi_id"] = random.randint(0, NUM_TAXI)
        record["payload"]["VendorID"] = random.randint(1, 2)

        record["payload"]["tpep_pickup_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        record["payload"]["tpep_dropoff_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        record["payload"]["passenger_count"] = random.randint(  0, 5)
        record["payload"]["trip_distance"] = random.uniform (0.0, 30.0)
        record["payload"]["RatecodeID"] = random.randint(  0, 2)
        record["payload"]["store_and_fwd_flag"] =random.choice(["Y","N"])
        record["payload"]["PULocationID"] = random.randint( 0, 300)
        record["payload"]["DOLocationID"] = random.randint( 0, 300)
        record["payload"]["payment_type"] = random.randint( 0, 70.0)
        record["payload"]["fare_amount"] = random.uniform ( 0, 70.0)
        record["payload"]["extra"] = random.uniform(0 , 2.5)
        record["payload"]["mta_tax"] = random.uniform (0, 0.5)
        record["payload"]["tip_amount"] = random.uniform(  0, 20.00)
        record["payload"]["tolls_amount"] = random.uniform(0, 6.55)
        record["payload"]["improvement_surcharge"] = random.uniform(-0.3, 0.3)
        record["payload"]["total_amount"] = random.randint(  0.00, 30.00)
        record["payload"]["congestion_surcharge"] = random.uniform(0 , 2.5)
        record["payload"]["airport_fee"] = random.uniform(0 , 2.5)

        topic_name = f'taxi_0'
        create_topic(admin, topic_name= topic_name)
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        print(record)
        sleep(2)

def teardown_stream(topic_name, servers=["localhost:9092"]): 
    try: 
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} is deleted")
    except Exception as e: 
        print(str(e))
        pass 

if __name__ == "__main__": 
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    print("Tearing down all existing topics")
    for taxi_id in range(NUM_TAXI): 
        try: 
            teardown_stream(f"taxi_{taxi_id}", [servers])
        except Exception as e: 
            print(f"Topic taxi_{taxi_id} does not exist..")
    
    if mode == "setup": 
        schemas_path = parsed_args['schemas_path']
        create_streams([servers], schemas_path)

        
