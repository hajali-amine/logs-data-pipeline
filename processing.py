from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.master("spark://localhost:7077").appName("example-pyspark-read-and-write").getOrCreate()
df_load = sparkSession.read.format("csv").option("header", "true").load('hdfs://localhost:50070/input')
df_load.show()
