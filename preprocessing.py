from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from kafka import KafkaConsumer
from pymongo.mongo_client import MongoClient
import json
import requests

def ip_to_country(ip):
        request_url = 'http://ip-api.com/json/' + ip
        response = requests.get(request_url)
        if len(response.content):
            result = response.content.decode()
            result  = json.loads(result)
            if result["status"] != "fail":
                return result['countryCode'] + " - " + result['country']
        return ""

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['log'])

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
collection = db['cleaned']

for msg in consumer:
    print (msg.value) 
    rdd = sc.parallelize([msg.value.decode("utf-8")])\
        .map(lambda x: x.split(" "))\
            .map(lambda x : (x[0], x[3][1:], x[5][1:] + x[6], x[8], x[9], x[10][1:-1], x[len(x) - 1][:-1]) )\
                .toDF(["ip", "date", "api", "status", "bytes", "referer", "time"])
    country = rdd.rdd.map(lambda x: (*x, ip_to_country(x[0])))\
        .filter(lambda x: x[len(x) - 1].find("Not found"))\
            .map(lambda x: (x[len(x) - 1], x[1], x[2], x[3], x[4], x[5], x[6]))\
                .toDF(["country", "date", "api", "status", "bytes", "referer", "time"])
    data = json.loads(country.toJSON().first())
    collection.insert_one(data)
    print("done")

# sparkSession = SparkSession.builder.appName("preprocessing").getOrCreate()
# # df_load = sparkSession.read.text("logfiles.log")
# # df_load.show()
# # splitted_file = df_load.rdd.map(lambda x: x[0])\
# #     .map(lambda x: x.split(" "))\
#         .map(lambda x : (x[0], x[3][1:], x[5][1:] + x[6], x[8], x[9], x[10][1:-1], x[len(x) - 1][:-1]) )\
#             .toDF(["ip", "date", "api", "status", "bytes", "referer", "time"])
# country = splitted_file.rdd.map(lambda x: (*x, ip_to_country(x[0])))\
#     .filter(lambda x: x[len(x) - 1].find("Not found"))\
#         .map(lambda x: (x[len(x) - 1], x[1], x[2], x[3], x[4], x[5], x[6]))\
#             .toDF(["country", "date", "api", "status", "bytes", "referer", "time"])
# country.show()

# ssc = StreamingContext(sparkSession.sparkContext, 1)
# ds = ssc.textFileStream("logfiles.log")

# ds.map(lambda x: print(x))

