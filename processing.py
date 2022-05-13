import json
from pyspark.sql import SparkSession, functions as F
from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
input_collection = db['cleaned']
country_stat_collection = db["country"]
api_stat_collection = db["api"]


spark = SparkSession.builder.appName("processing").getOrCreate()

data = list(input_collection.find({}, {"_id":0}))
df_data= spark.createDataFrame(data)
df_data.show()
size = df_data.count()

df_country_count = df_data.groupBy("country").count().withColumn("percent", F.col("count") / size)
country_records = df_country_count.toPandas().to_dict(orient="records")
country_stat_collection.insert_many(country_records)

df_api_perf_eval = df_data.groupBy("api").agg(F.min("time"), F.max("time"), F.avg("time"))
api_records = df_api_perf_eval.toPandas().to_dict(orient="records")
api_stat_collection.insert_many(api_records)