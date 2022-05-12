from kafka import KafkaProducer, KafkaConsumer
from json import dumps

producer = KafkaProducer(bootstrap_servers='localhost:9092',group_id=None,value_serializer=lambda x: dump(x).encode('utf-8'))

logs = open("logfiles.log", "r")

for line in logs:
    #print(line)
    producer.send('log', str.encode(line))
    print("line sent")
logs.close()

