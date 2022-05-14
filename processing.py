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
df_country_count.show()
"""
+-------------------+-----+--------------------+                                
|            country|count|             percent|
+-------------------+-----+--------------------+
| US - United States|   10| 0.29411764705882354|
|     ID - Indonesia|    1|0.029411764705882353|
|     HK - Hong Kong|    1|0.029411764705882353|
|       BE - Belgium|    1|0.029411764705882353|
|     AU - Australia|    2|0.058823529411764705|
|         CN - China|    4| 0.11764705882352941|
|        CA - Canada|    1|0.029411764705882353|
|   KR - South Korea|    1|0.029411764705882353|
|        FR - France|    1|0.029411764705882353|
|         JP - Japan|    3| 0.08823529411764706|
|         IT - Italy|    1|0.029411764705882353|
|     AR - Argentina|    1|0.029411764705882353|
|         ES - Spain|    1|0.029411764705882353|
|        BR - Brazil|    1|0.029411764705882353|
|       CZ - Czechia|    1|0.029411764705882353|
|        PL - Poland|    2|0.058823529411764705|
|GB - United Kingdom|    1|0.029411764705882353|
|  ZA - South Africa|    1|0.029411764705882353|
+-------------------+-----+--------------------+
"""
country_records = df_country_count.toPandas().to_dict(orient="records")
country_stat_collection.insert_many(country_records)
print("country stats persisted")

# Get the min, max and avg response time per api and store it to a collection named 'api' in Mongo
df_api_perf_eval = df_data.groupBy("api").agg(F.min("time"), F.max("time"), F.avg("time"))\
    .withColumnRenamed("avg(time)", "avg").withColumnRenamed("min(time)", "min").withColumnRenamed("max(time)", "max")
df_api_perf_eval.show()
"""
+--------------------+----+----+------------------+
|                 api| min| max|               avg|
+--------------------+----+----+------------------+
|          DELETE/usr|2338|3899|            3118.5|
|    DELETE/usr/admin|2611| 492|            1551.5|
|DELETE/usr/admin/...|2577|4479|3236.6666666666665|
|    DELETE/usr/login|4478|4478|            4478.0|
| DELETE/usr/register|3079|3079|            3079.0|
|             GET/usr|1146|3056|2023.6666666666667|
|GET/usr/admin/dev...|1457|4909|3704.6666666666665|
|       GET/usr/login|3388| 988|            3707.6|
|    GET/usr/register| 238| 238|             238.0|
|            POST/usr|1118| 887|           1904.25|
|      POST/usr/admin| 661| 661|             661.0|
|POST/usr/admin/de...|1859|3930|            2894.5|
|       PUT/usr/admin|2264|4816|            3540.0|
|PUT/usr/admin/dev...|3794|4585|            4292.0|
|       PUT/usr/login| 990| 990|             990.0|
+--------------------+----+----+------------------+
"""
api_records = df_api_perf_eval.toPandas().to_dict(orient="records")
api_stat_collection.insert_many(api_records)
print("api stats persisted")