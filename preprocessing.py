from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random


spark = SparkSession \
          .builder \
          .appName("Preprocessor") \
          .getOrCreate()

# Subscribe to 1 topic
# df = spark \
#   .read \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "172.27.1.16:9092") \
#   .option("subscribe", "log") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "log") \
  .option("startingOffsets", "earliest") \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
