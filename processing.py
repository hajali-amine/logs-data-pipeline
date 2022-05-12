from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as F

sparkSession = SparkSession.builder.appName("processing").getOrCreate()
df_load = sparkSession.read.format("csv").option("header", "true").load('out.csv')
size = df_load.count()
df_country_count = df_load.groupBy("country").count().withColumn("percent", F.col("count") / size)
df_country_count.show()
df_api_perf_eval = df_load.groupBy("api").agg(F.min("time"), F.max("time"), F.avg("time"))
df_api_perf_eval.show()