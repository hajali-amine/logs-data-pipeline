from kafka import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer('log',bootstrap_servers='localhost:9092')
consumer.subscribe('log')
for msg in consumer:
    print (msg.value)

#from pyspark import SparkContext
#sc = SparkContext()
#ma_liste = range(10000)
#rdd = sc.parallelize(ma_liste, 2)
#nombres_impairs = rdd.filter(lambda x: x % 2 != 0)
#nombres_impairs.take(5)
