from flask import Flask, jsonify
from pymongo.mongo_client import MongoClient

mongoClient = MongoClient('localhost', 26000)
db = mongoClient['logs']
country_stat_collection = db["country"]
api_stat_collection = db["api"]

app = Flask(__name__)

@app.route("/country")
def country():
    # Get country stats
    data = jsonify(list(country_stat_collection.find({}, {"_id":0})))
    data.headers.add('Access-Control-Allow-Origin', '*')
    return data

@app.route("/api")
def api():
    # Get api stats
    data = jsonify(list(api_stat_collection.find({}, {"_id":0})))
    data.headers.add('Access-Control-Allow-Origin', '*')
    return data
