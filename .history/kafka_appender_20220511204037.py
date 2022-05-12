from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        group_id=None,
                        auto_commit_enable=False,
                        auto_offset_reset ='earliest')

logs = open("logfiles.log", "r")

for line in logs:
    #print(line)
    producer.send('log', str.encode(line))
    print("line sent")
logs.close()

