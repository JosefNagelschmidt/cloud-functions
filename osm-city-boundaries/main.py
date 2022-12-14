import json
from datetime import date
from io import StringIO

import pandas as pd
import requests
from google.cloud import storage
from osm2geojson import json2geojson


def load_city_boundaries(event, context):
    """Cloud Function to be triggered by Cloud Storage.
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

    # read city config
    bucket = storage_client.bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    query_config = json.loads(blob.download_as_string(client=storage_client))

    # retrieve city boundaries from OSM API
    query = f"https://overpass-api.de/api/interpreter?data=[out:json][timeout:60];relation['name'='{query_config['city']}']['admin_level'='{query_config['admin_level']}'];out body;>;out skel qt;"
    geodata = requests.get(query).json()
    transformed_geodata = json2geojson(data=geodata)

    # write city boundaries to GCS
    bucket = storage_client.bucket("bucket-osm-cities-9755a02f7829dc9a")
    blob_name = f"{query_config['city']}-{query_config['admin_level']}-{str(date.today())}.geojson"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(transformed_geodata))

    # read in full population density file from GCS
    bucket = storage_client.bucket("bucket-general-config-files")
    data_blob = bucket.blob("pop_density.csv")
    data_str = data_blob.download_as_text()
    pop_density = pd.read_csv(StringIO(data_str))
    # filter only relevant rows for city
    pop_density = pop_density[pop_density["city"] == query_config['city_official_name']]
    # add helper col for empirical population distribution
    pop_density["density"] = pop_density["population"] / pop_density["population"].sum()
    # write filtered population density to GCS
    bucket = storage_client.bucket("bucket-city-population-grids")
    blob_name = f"pop_density_{query_config['city']}-{str(date.today())}.csv"
    bucket.blob(blob_name).upload_from_string(pop_density.to_csv(index=False), 'text/csv')
