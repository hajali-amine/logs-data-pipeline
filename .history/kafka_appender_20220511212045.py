from kafka import KafkaProducer, KafkaConsumer
from json import dumps
KAFKA_TOPIC = 'topic1' 

producer = KafkaProducer(KAFKA_TOPIC ,bootstrap_servers='localhost:9092')

logs = open("logfiles.log", "r")

for line in logs:
    #print(line)
    rep =producer.send('log', str.encode(line))
    print (rep)
logs.close()
producer.flush()

