from pyspark.sql import SparkSession, functions as F
from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)

db = mongoClient['logs']
input_collection = db['cleaned']
country_stat_collection = db["country"]
api_stat_collection = db["api"]

spark = SparkSession.builder.appName("processing").getOrCreate()

# Get the preprocessed data from Mongo
data = list(input_collection.find({}, {"_id":0}))

# Convert the data into a Spark Dataframe
df_data= spark.createDataFrame(data)

# Get the number of rows
size = df_data.count()

# Get the percentage of number of request per country and store it to a collection named 'country' in Mongo
df_country_count = df_data.groupBy("country").count().withColumn("percent", F.col("count") / size)
country_records = df_country_count.toPandas().to_dict(orient="records")
country_stat_collection.insert_many(country_records)

# Get the min, max and avg response time per api and store it to a collection named 'api' in Mongo
df_api_perf_eval = df_data.groupBy("api").agg(F.min("time"), F.max("time"), F.avg("time"))\
    .withColumnRenamed("avg(time)", "avg").withColumnRenamed("min(time)", "min").withColumnRenamed("max(time)", "max")
api_records = df_api_perf_eval.toPandas().to_dict(orient="records")
api_stat_collection.insert_many(api_records)