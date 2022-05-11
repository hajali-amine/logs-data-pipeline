from kafka import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['log'])
for msg in consumer:
    print (msg.value)