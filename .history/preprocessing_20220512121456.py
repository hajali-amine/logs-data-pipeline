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




from pyspark import SparkContext
import csv

import pyspark.sql.functions
sc = SparkContext()
rdd = sc.textFile("logfiles.log").map(lambda line: line.split(" ")).filter(lambda line: len(line)<=1).collect()