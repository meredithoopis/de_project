from pyspark.sql import SparkSession 
import argparse 
import os 
from dotenv import load_dotenv 
from pydeequ.profiles import ColumnProfilerRunner 

load_dotenv("../.env")

def main(): 
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars", "../jars/postgresql-42.6.0.jar,../jars/deequ-2.0.3-spark-3.3.jar")
        .appName("Spark example").config("spark.some.config.options", "some-value")
        .getOrCreate()
    )
    df = (
        spark.read.format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", f"jdbc:postgresql:{os.getenv('POSTGRES_DB')}")
        .option("dbtable", "public.taxis")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD")).load()
    )
    #Using deequ to test data quality 
    res = ColumnProfilerRunner(spark).onData(df).run()
    for col, profile in res.profiles.items(): 
        if col == "index": 
            continue 
        print("*" * 40)
        print(f"Column: {col}")
        print(profile)
    
if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", default = "../data", help = "Store data in parquet format."
    )
    args = parser.parse_args()
    main(args)