from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
collection = db['cleaned']
collection.delete_many({})
a = collection.find()
for doc in a:
    print(doc)