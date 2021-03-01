import pymongo
import json

filepath = 'Load_in_database/results.json'
connection = pymongo.MongoClient()
db = connection['user_data']
collection_banners = db['banners']


def load_data_in_mongo(filepath, collection_banners):
    with open(filepath) as f:
        file_data = json.load(f)
    collection_banners.insert(file_data)
    return 1


def query_data_from_mongo(campaign_id, time_quarter, collection_banners):
    doc = list(collection_banners.find({'campaign_id': campaign_id, 'time_quarter': time_quarter}))
    return (doc[0]['list_of_banners'])
