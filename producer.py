from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

logs = open("logfiles.log", "r")

# Append every line in the logfiles.log file to the Kafka topic 'log'
for line in logs:
    producer.send('log', str.encode(line))

logs.close()
producer.flush()