from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

#logs = open("logfiles.log", "r")
logs = ['233.223.117.90 - - [27/Dec/2037:12:00:00 +0530] "DELETE /usr/admin HTTP/1.0" 502 4963 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4380.0 Safari/537.36 Edg/89.0.759.0" 45',
'162.253.4.179 - - [27/Dec/2037:12:00:00 +0530] "GET /usr/admin/developer HTTP/1.0" 200 5041 "http://www.parker-miller.org/tag/list/list/privacy/" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36" 3885,'
'252.156.232.172 - - [27/Dec/2037:12:00:00 +0530] "POST /usr/register HTTP/1.0" 404 5028 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 OPR/73.0.3856.329" 3350',
'182.215.249.159 - - [27/Dec/2037:12:00:00 +0530] "PUT /usr/register HTTP/1.0" 304 4936 "http://www.parker-miller.org/tag/list/list/privacy/" "Mozilla/5.0 (Android 10; Mobile; rv:84.0) Gecko/84.0 Firefox/84.0" 767',
]
for line in logs:
    producer.send('log', str.encode(line))
    print("line sent")
#logs.close()

