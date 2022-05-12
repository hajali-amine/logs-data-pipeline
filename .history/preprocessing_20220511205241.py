from kafka import KafkaProducer, KafkaConsumer
from json import loads

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
consumer.subscribe(['log'])
for msg in consumer:
    print (msg.value)

#from pyspark import SparkContext
#sc = SparkContext()
#ma_liste = range(10000)
#rdd = sc.parallelize(ma_liste, 2)
#nombres_impairs = rdd.filter(lambda x: x % 2 != 0)
#nombres_impairs.take(5)
