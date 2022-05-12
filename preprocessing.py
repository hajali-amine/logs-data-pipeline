#https://github.com/Parsely/pykafka/issues/334
#https://github.com/Parsely/pykafka
#from kafka import KafkaProducer, KafkaConsumer
#from json import loads
#from time import sleep
#from kafka import TopicPartition


#consumer = KafkaConsumer('log', bootstrap_servers='localhost:9092',
#     enable_auto_commit=True,
#     auto_offset_reset='smallest',
#    )
#rep =consumer.subscribe(['log'])
#print(rep)
#for msg in consumer:
#    print (msg.value)




from pyspark.sql import SparkSession
  
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
  
df = spark.read.text("output.txt")
  
df.selectExpr("split(value, ' ') as\
Text_Data_In_Rows_Using_Text").show(4,False)
