from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

logs = open("logs.txt", "r")
for line in logs:
    producer.send('log', str.encode(line))
logs.close()

