from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
df_load = sparkSession.read.format("csv").option("header", "true").load('hdfs://namenode:50070/input/out.csv')
df_load.show()
