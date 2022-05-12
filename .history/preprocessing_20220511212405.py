#https://github.com/Parsely/pykafka/issues/334
#https://github.com/Parsely/pykafka
from kafka import KafkaProducer, KafkaConsumer
from json import loads
from time import sleep
from kafka import TopicPartition


KAFKA_TOPIC = 'topic1' 
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers='localhost:9092',
     enable_auto_commit=True,
     auto_offset_reset='smallest',
    )


rep =consumer.subscribe(['log'])
print(rep)
for msg in consumer:
    print (msg.value)

#from pyspark import SparkContext
#sc = SparkContext()
#ma_liste = range(10000)
#rdd = sc.parallelize(ma_liste, 2)
#nombres_impairs = rdd.filter(lambda x: x % 2 != 0)
#nombres_impairs.take(5)
