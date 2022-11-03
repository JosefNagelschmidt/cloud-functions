# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from datetime import date

import functions_framework
import requests
from google.cloud import storage
from osm2geojson import json2geojson


def hello_gcs(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed,
       and works for all Cloud Storage CRUD operations.
    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Cloud Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    storage_client = storage.Client()
    bucket = storage_client.bucket(event['bucket'])
    blob = bucket.blob(event['name'])

    query_config = json.loads(blob.download_as_string(client=storage_client))
    geodata = requests.get(f'https://overpass-api.de/api/interpreter?data=[out:json][timeout:60];relation["name"={query_config["city"]}]["admin_level"={query_config["admin_level"]}];out body;>;out skel qt;').json()
    transformed_geodata = json2geojson(data=geodata)

    bucket = storage_client.bucket("bucket-osm-cities-9755a02f7829dc9a")
    blob = bucket.blob(f'{query_config["city"]}-{query_config["admin_level"]}-{str(date.today())}.geojson')
    blob.upload_from_string(json.dumps(transformed_geodata))




