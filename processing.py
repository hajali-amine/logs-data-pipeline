from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
df_load = sparkSession.read.format("csv").option("header", "true").load('out.csv')
df_country_sum = df_load.groupBy("country").count()
df_country_sum.show()
