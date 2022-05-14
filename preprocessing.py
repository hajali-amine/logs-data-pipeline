from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from kafka import KafkaConsumer
from pymongo.mongo_client import MongoClient
import json
import requests

# A methods that returns the country from an IP address
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

# Kafka consumer subscribes to the topic 'log'
consumer.subscribe(['log'])

sc = SparkContext.getOrCreate()
spark = SparkSession(sc) # This allows us to use Spark Dataframes

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
collection = db['cleaned']

# Stream treatment
for raw_data in consumer:
    #Change line into and RDD -> Split line into array -> Remove " and [ and unnecessary data -> Convert to Dataframe
    cleaned_data = sc.parallelize([raw_data.value.decode("utf-8")])\
        .map(lambda x: x.split(" "))\
            .map(lambda x : (x[0], x[3][1:], x[5][1:] + x[6], x[8], x[9], x[10][1:-1], x[len(x) - 1][:-1]) )\
                .toDF(["ip", "date", "api", "status", "bytes", "referer", "time"])
    cleaned_data.show()
    """
    +--------------+--------------------+--------------------+------+-----+--------------------+----+
    |            ip|                date|                 api|status|bytes|             referer|time|
    +--------------+--------------------+--------------------+------+-----+--------------------+----+
    |163.115.129.93|27/Dec/2037:12:00:00|PUT/usr/admin/dev...|   500| 4964|http://www.parker...|1270|
    +--------------+--------------------+--------------------+------+-----+--------------------+----+
    """
    
    # Get country from IP -> Filter line without country - [if not empty] > Remove the IP -> Convert to Dataframe
    data_with_country = cleaned_data.rdd.map(lambda x: (*x, ip_to_country(x[0])))
    data_with_country.toDF().show()
    """
    +--------------+--------------------+--------------------+---+----+--------------------+----+-----------+
    |            _1|                  _2|                  _3| _4|  _5|                  _6|  _7|         _8|
    +--------------+--------------------+--------------------+---+----+--------------------+----+-----------+
    |163.115.129.93|27/Dec/2037:12:00:00|PUT/usr/admin/dev...|500|4964|http://www.parker...|1270|FR - France|
    +--------------+--------------------+--------------------+---+----+--------------------+----+-----------+
    """

    data_with_country_filtered = data_with_country.filter(lambda x: x[len(x) - 1].strip())

    if not data_with_country_filtered.isEmpty():
        ready_to_save = data_with_country_filtered.map(lambda x: (x[len(x) - 1], x[1], x[2], x[3], x[4], x[5], x[6]))\
                .toDF(["country", "date", "api", "status", "bytes", "referer", "time"])
        ready_to_save.show()
        """
        +-----------+--------------------+--------------------+------+-----+--------------------+----+
        |    country|                date|                 api|status|bytes|             referer|time|
        +-----------+--------------------+--------------------+------+-----+--------------------+----+
        |FR - France|27/Dec/2037:12:00:00|PUT/usr/admin/dev...|   500| 4964|http://www.parker...|1270|
        +-----------+--------------------+--------------------+------+-----+--------------------+----+
        """

        # Convert Spark Dataframe to Json
        data = json.loads(ready_to_save.toJSON().first())

        # Store data
        collection.insert_one(data)
        print(f"line inserted - [{data}]")
