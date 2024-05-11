import os

import json
import logging
import time
import argparse
import IP2Location
import apache_beam as beam

from os.path import join, dirname
from dotenv import load_dotenv

from datetime import datetime
from apache_beam.io import fileio
from apache_beam.io import mongodbio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.io.mongodbio import WriteToMongoDB

from google.cloud import storage
from typing import NamedTuple, List, Optional
from pymongo import MongoClient


dotenv_path = join('/home/kafka/GCP/resource/', '.env')

load_dotenv(dotenv_path)

os.environ['GOOGLE_APPLICATION_CREDENTIALS']

class Option(NamedTuple):
    option_label: str
    option_id: str
    value_label: str
    value_id: str

class EventLog(NamedTuple):
    time_stamp: int
    ip: str
    user_agent: str
    resolution: str
    user_id_db: str
    device_id: str
    api_version: str
    store_id: str
    local_time: str
    show_recommendation: str
    current_url: str
    referrer_url: str
    email_address: str
    recommendation: bool
    utm_source: bool
    utm_medium: bool
    collection: str
    product_id: str
    option: List[Option]
    
    # new fields
    order_id: Optional[int]
    cart_products: Optional[List[Option]]
    cat_id: Optional[int]
    collect_id: Optional[str]
    viewing_product_id: Optional[str]
    recommendation_product_id: Optional[str]
    recommendation_clicked_position: Optional[int]
    price: Optional[str]
    currency: Optional[str]
    is_paypal: Optional[str]
    key_search: Optional[str]

beam.coders.registry.register_coder(EventLog, beam.coders.RowCoder)

class ConvertToEventLogFn(beam.DoFn):

    schema = {}

    def setup(self):
        try:
            # Download file schema db location in gcs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket("data1projecttest")
            blob = bucket.blob("schema/default.json")
            schema = blob.download_as_string(client=None)

            self.schema = json.loads(schema)
        except:
            raise ValueError("Failed to start ConvertToEventLogFn Bundle.")

    def process(self, element):
        try:
            row = json.loads(element.decode('utf-8'))
            for key in (self.schema).keys():
                if key not in row: 
                    row[key] = self.schema[key]
            yield beam.pvalue.TaggedOutput('parsed_row', row)
        except:
            if(element is None):
                yield beam.pvalue.TaggedOutput('unparsed_row', "")
            else:
                yield beam.pvalue.TaggedOutput('unparsed_row', element.decode('utf-8'))

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        yield json.dumps(element)


class UpdateMongoDb(beam.DoFn):
    def __init__(self, mongo_url):
        self.mongo_url = mongo_url
        
    def connectMongoDB(self):
        try:
            self.mongodbClient = MongoClient(self.mongo_url)
        except Exception as e:
            raise ValueError("Failed to connect to MongoDB: {}".format(str(e)))
        
    def setup(self):
        try:
            self.connectMongoDB()
        except ValueError as e:
            print("Setup UpdateMongoDb failed:", e)

    def process(self, element, window=beam.DoFn.WindowParam):
        database = self.mongodbClient['testdb']
        collection = database['output']
        
        collection.update_one({
            "_id": element["_id"]
        }, {
            "$set": {
                "URL": element["URL"]
                }
        })

    def teardown(self):
        if self.mongodbClient is not None:
            self.mongodbClient.close()


class TransformBeforeToMongDBFn(beam.DoFn):
    
    def __init__(self, mongo_url):
        self.databaseIPs = None
        self.mongo_url = mongo_url
        
    def getDatabaseLocationFile(self):
        try:
            
            storage_client = storage.Client()
            bucket = storage_client.get_bucket("data1projecttest")
            blob = bucket.blob("location/IP-COUNTRY-REGION-CITY.BIN")
            blob.download_to_filename("IP-COUNTRY-REGION-CITY.BIN")
            
            if(os.path.isfile("IP-COUNTRY-REGION-CITY.BIN")):
                
                self.databaseIPs = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")
            else:
                raise ValueError("File IP-COUNTRY.BIN download not exists TransformBeforeToMongoDBFn Bundle.")
            
        except:
            raise ValueError("File IP-COUNTRY.BIN failed to download IP-COUNTRY.BIN TransformBeforeToMongoDBFn Bundle.")

    def connectMongoDB(self):
        try:
            self.mongodbClient = MongoClient(self.mongo_url)
        except:
            raise ValueError("Failed to Connect Mongo TransformBeforeToMongoDBFn Bundle.")
        
    def setup(self):
        try:
            self.getDatabaseLocationFile()
            self.connectMongoDB()
        except ValueError:
            logging.error("Setup TransformBeforeToMongDBFn failed")
            
    def process(self, element, window=beam.DoFn.WindowParam):
        row = element

        if(row['device_id'] is None):
            yield beam.pvalue.TaggedOutput('no_device_id', row)
        if(row['ip'] is not None):
            rec = self.databaseIPs.get_all(row['ip'])
            row['country'] = rec.country_long

        database = self.mongodbClient[os.environ['DATABASE_OUTPUT']]
        collection = database[os.environ['COLLECTION_OUTPUT']]

        device = collection.find_one({
            'ip' : element["ip"],
            'user_id_db' : element["user_id_db"],
            'device_id' : element["device_id"],
            "country" : element["country"]
        })

        if(device):

            urls = device["URL"]  # Retrieve the URL list, initialize to an empty list if not present
            url_current = element["current_url"]
            
            if url_current not in urls:
                urls.append(url_current)
                device["URL"] = urls
        
            yield beam.pvalue.TaggedOutput('update_device_id_event', device)
            
        else:

            event = {
                "ip" : row["ip"],
                "user_id_db" : row["user_id_db"],
                "device_id" : row["device_id"],
                "country" : row["country"],
                "URL" : [row["current_url"]]
            }
            
            yield beam.pvalue.TaggedOutput('create_device_id_event', event)

    def teardown(self):
        if(self.mongodbClient is not None):
            self.mongodbClient.close()

def run():

    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into Google Cloud Storage and MongoDB')

    parser.add_argument('--project', required=True, help='Project')
    parser.add_argument('--region', required=True, help='Region')
    parser.add_argument('--topic', required=True, help='Topic Pub/Sub')
    parser.add_argument('--staging_location', required=True, help='Staging Location')
    parser.add_argument('--temp_location', required=True, help='Temp Location')

    parser.add_argument('--window_duration', required=True, help='Window duration in seconds')
    parser.add_argument('--allowed_lateness', required=True, help='Allowed lateness')
    parser.add_argument('--output_bucket', required=True, help='GCS Output')
    parser.add_argument('--dead_letter_bucket', required=True, help='GCS Dead Letter Bucket')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--mongo_url', required=True, help='URL MongoDB')

    opts, pipeline_opts = parser.parse_known_args()

    pipeline_opts.append("--max_num_workers=2")
    pipeline_opts.append("--save_main_session")
    pipeline_opts.append("--streaming")
    pipeline_opts.append("--allow_unsafe_triggers")
    pipeline_opts.append("--sdk_location=container")

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('glamira-streaming-event-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    topic = opts.topic
    output_path = opts.output_bucket
    output_error_path = opts.dead_letter_bucket

    mongo_url = opts.mongo_url

    window_duration = opts.window_duration
    allowed_lateness = opts.allowed_lateness

    p = beam.Pipeline(options=options)

    # 1. Subscription message from PubSub
    # 2. Parse JSON 2 format
    rows = (p 
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic)
        | 'ParseJson' >> beam.ParDo(ConvertToEventLogFn()).with_outputs('parsed_row', 'unparsed_row') 
    )

    # For unparsed format:
    # 1. Each 10s call message one time
    # 2.  
    (rows.unparsed_row
        | 'BatchOver10s' >> beam.WindowInto(beam.window.FixedWindows(120), trigger=AfterProcessingTime(120), accumulation_mode=AccumulationMode.DISCARDING)
        | 'WriteUnparsedToGCS' >> fileio.WriteToFiles(output_error_path, shards=1, max_writers_per_bundle=0)
    )

    # For parsed format:
    window_transforms = (rows.parsed_row
        | "WindowByMinute" >> beam.WindowInto(beam.window.FixedWindows(int(window_duration)), trigger=AfterWatermark(late=AfterCount(1)), allowed_lateness=int(allowed_lateness), accumulation_mode=AccumulationMode.ACCUMULATING)
        # | "CountPerMinute" >> beam.CombineGlobally(CountCombineFn()).without_defaults()
        # | "AddWindowTimestamp" >> beam.ParDo(GetTimestampFn())
    )

    (window_transforms 
        | "TransformBeforeToGCS" >> beam.ParDo(GetTimestampFn())
        | 'WriteparsedToGCS' >> fileio.WriteToFiles(output_path, shards=1, max_writers_per_bundle=0)
    )
    
    mongo_transformed = (window_transforms
        | "TransformBeforeToMongDB" >> beam.ParDo(TransformBeforeToMongDBFn(mongo_url)).with_outputs('no_device_id', 'update_device_id_event', 'create_device_id_event')
    )
    
    (mongo_transformed.no_device_id
        | "WriteNoDeviceIdToGCS" >> fileio.WriteToFiles(output_error_path, shards=1, max_writers_per_bundle=0)
    )
    
    (mongo_transformed.update_device_id_event
        | "UpsertParsedToMongoDB" >> beam.ParDo(UpdateMongoDb(mongo_url))
    )
    
    (mongo_transformed.create_device_id_event
        | "WriteParsedToMongoDB" >> WriteToMongoDB(uri=mongo_url, db=os.environ['DATABASE_OUTPUT'], coll=os.environ['COLLECTION_OUTPUT'])
    )


    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()