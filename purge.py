from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)

db = mongoClient['logs']
collection = db['cleaned']
country_stat_collection = db["country"]
api_stat_collection = db["api"]

# Deletes the data from all of the collections
collection.delete_many({})
country_stat_collection.delete_many({})
api_stat_collection.delete_many({})
