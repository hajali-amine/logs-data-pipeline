from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
collection = db['cleaned']
country_stat_collection = db["country"]
api_stat_collection = db["api"]
# collection.delete_many({})
a = country_stat_collection.find()
for doc in a:
    print(doc)