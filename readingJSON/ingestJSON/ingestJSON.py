import ijson
import configparser
from pymongo import MongoClient
import math
import os

config = configparser.RawConfigParser()
config.read(r'ingestJSON/config.properties')


def get_mongo_collection():
    host = config.get('DatabaseSection', 'mongo.host')
    port = int(config.get('DatabaseSection', 'mongo.port'))
    db_name = config.get('DatabaseSection', 'mongo.dbname')
    col_name = config.get('DatabaseSection', 'mongo.collection')

    conn = MongoClient(host, port)
    mongo_collection = conn[db_name][col_name]

    return mongo_collection


def get_health_risk(bmi):
    if bmi < 18.5:
        return 'Malnutrition risk'
    if 18.5 <= bmi < 25:
        return 'Low risk'
    if 25 <= bmi < 30:
        return 'Enhanced risk'
    if 30 <= bmi < 35:
        return 'Medium risk'
    if 35 <= bmi < 40:
        return 'High risk'
    return 'Very High risk'


def get_bmi(weight, height):
    return float(weight / math.pow(height/100.0, 2))


def populate_health_collection(file_path):
    collection = get_mongo_collection()
    file_arr = [file_path+"/"+a for a in os.listdir(file_path)]

    for file in file_arr:
        with open(file, 'r') as p:

            print("Will ingest file {}".format(file))
            objects = ijson.items(p, 'item')

            for obj in objects:
                obj['BMI'] = get_bmi(obj['WeightKg'], obj['HeightCm'])
                obj['HealthRisk'] = get_health_risk(obj['BMI'])

                # print('Object: {}'.format(obj))
                collection.insert_one(obj)

        cursor = collection.find()
        print("After ingesting {}, number of records in collection are: {}".format(file, cursor.count()))

