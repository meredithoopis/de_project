from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg 

def main(): 
    spark = SparkSession.builder.master("local[*]").appName("Query spark table").getOrCreate()
    data = spark.read.parquet("/home/meredith/apps/cap/data/taxi-data/0-0d57ccff-7c4e-4da3-aba9-81abb3c3685d-0.parquet")
    data.createOrReplaceTempView("data")
    query = spark.sql("SELECT * FROM data WHERE passenger_count > 0")
    query.show()

if __name__ == "__main__": 
    main()