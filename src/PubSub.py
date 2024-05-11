import os
import json

from google.cloud import pubsub_v1
from pymongo import MongoClient
from time import sleep
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join('/home/kafka/GCP/resource/', '.env')

load_dotenv(dotenv_path)

os.environ['GOOGLE_APPLICATION_CREDENTIALS']

mg_client = MongoClient(os.environ['MONGDODB_DEV'])
mg_db = mg_client[os.environ['DATABASE_NAME']]
mg_collection = mg_db[os.environ['COLLECTION_INPUT']]

lastTimeStamp = None

LIMIT=100

project_id = os.environ['PROJECT_ID']
topic_id = os.environ['TOPIC_ID']

# Connect to Pub/Sub
publisher = pubsub_v1.PublisherClient()

try:
    fileReadLastTimeStamp = open("LastTimeStamp","r")
    lastTimeStamp = fileReadLastTimeStamp.read()
except:
    lastTimeStamp = None

if lastTimeStamp:
    documents = mg_collection.find({"time_stamp": {"$gt": int(lastTimeStamp)}}).limit(LIMIT).sort("time_stamp", 1)
else:
    documents = mg_collection.find().limit(LIMIT).sort("time_stamp", 1)

results = list(documents.clone())

# Query and puslish data
while(len(results) > 0):
    print("=================================")
    if lastTimeStamp is not None:
        print("Start send Pub/Sub from Timestamp: " + str(lastTimeStamp))
    else:
        print("Start send Pub/Sub from Timestamp from Beginning")

    for doc in results:
        lastTimeStamp = doc['time_stamp']
        doc.pop("_id")
        if 'option' in doc and 'category id' in doc['option']:
            doc['option']['category_id'] = doc['option'].pop('category id')

        data = (json.dumps(doc)).encode("utf-8")
        print("lastTimeStamp: " + str(lastTimeStamp) + " sent")

        topic_path = publisher.topic_path(project_id, topic_id)
        future = publisher.publish(topic_path, data)
        print(f"{future.result()}")

    try:
        fWriteLastTimeStamp = open("LastTimeStamp", "w")
        fWriteLastTimeStamp.write(str(lastTimeStamp))
        fWriteLastTimeStamp.close()
    except:
        lastTimeStamp = None

    sleep(60)   

    if lastTimeStamp:
        documents = mg_collection.find({"time_stamp": {"$gt": int(lastTimeStamp)}}).limit(LIMIT).sort("time_stamp", 1)
    else:
        documents = mg_collection.find().limit(LIMIT).sort("time_stamp", 1)

    results = list(documents.clone())

print('Done!')