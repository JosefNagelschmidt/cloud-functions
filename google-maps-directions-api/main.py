import json
import sys
from datetime import date

import requests
from google.cloud import storage
from shapely.geometry import MultiPolygon, shape


def list_blobs(client, bucket_name):
    """Lists all the blobs in the bucket."""
    blobs = client.list_blobs(bucket_name)
    return [blob.name for blob in blobs]


def hello_pubsub(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    bucket_name = 'bucket-osm-cities-9755a02f7829dc9a'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    file_list = list_blobs(client=storage_client,bucket_name=bucket_name)

    for file in file_list:
        blob = bucket.blob(file)
        features = json.loads(blob.download_as_string(client=storage_client))["features"]
        geo = [feature["geometry"] for feature in features if feature["geometry"]["type"] == "MultiPolygon"]
        if len(geo) != 1:
            sys.exit()

        polygon: MultiPolygon = shape(geo[0])
        
