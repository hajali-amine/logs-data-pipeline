from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

logs = open("logfiles.log", "r")

for line in logs:
    #print(line)
    producer.send('log', str.encode(line))
    print("line sent")
logs.close()

