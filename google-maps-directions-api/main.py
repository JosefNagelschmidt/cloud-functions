import sys
from datetime import datetime, timedelta
from io import StringIO
from random import random
import os

import googlemaps
import pandas as pd
import requests
from geopy import Point
from geopy.distance import geodesic
from google.cloud import storage
from google.cloud import bigquery
import logging


def list_blobs(client, bucket_name):
    """Lists all the blobs in the bucket."""
    blobs = client.list_blobs(bucket_name)
    return [blob.name for blob in blobs]


def generate_point_in_neighborhood(center: Point, radius: int) -> Point:
    radius_in_kilometers = radius * 1e-3
    random_distance = random() * radius_in_kilometers
    random_bearing = random() * 360
    return geodesic(kilometers=random_distance).destination(center, random_bearing)


def load_city_grid(bucket, file) -> pd.DataFrame:
    data_str = bucket.blob(file).download_as_text()
    return pd.read_csv(StringIO(data_str))


def generate_weighted_pairs_of_points(bucket_city_grids, grid_file):

    df = load_city_grid(bucket=bucket_city_grids, file=grid_file)
    sample = df.sample(n=2, weights="density")
    res = [
        generate_point_in_neighborhood(
            center=Point(row["latitude"], row["longitude"]), radius=500
        )
        for _, row in sample.iterrows()
    ]
    return res


def enrich_point(point: Point) -> dict:
    res = {}
    res["latitude"], res["longitude"], altitude = point

    query = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={res['latitude']}&lon={res['longitude']}"

    try:
        r = requests.get(query)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.response.text)
        sys.exit()

    geodata = r.json()

    res.update(geodata["address"])
    del res["ISO3166-2-lvl4"]
    return res


def generate_meta_table_rows(enriched_points: list[dict]):
    insert_meta_table = []

    id_origin = datetime.now()
    id_destination = id_origin + timedelta(seconds=1)

    origin = {"id": id_origin, "type": "origin"}
    destination = {"id": id_destination, "type": "destination"}
    origin.update(enriched_points[0])
    destination.update(enriched_points[1])

    insert_meta_table.append(origin)
    insert_meta_table.append(destination)

    return insert_meta_table, id_origin, id_destination


def generate_distance_table_rows(
    id_origin: datetime,
    id_destination: datetime,
    client: googlemaps.Client,
    enriched_points: list[dict],
):
    now = datetime.now()

    # DRIVING
    driving = client.distance_matrix(
        origins=[
            (enriched_points[0].get("latitude"), enriched_points[0].get("longitude"))
        ],
        destinations=[
            (enriched_points[1].get("latitude"), enriched_points[1].get("longitude"))
        ],
        mode="driving",
        units="metric",
        departure_time=now,
    )
    driving_duration_in_s = (
        driving.get("rows")[0]
        .get("elements")[0]
        .get("duration_in_traffic")
        .get("value")
    )
    driving_distance_in_m = (
        driving.get("rows")[0].get("elements")[0].get("distance").get("value")
    )
    # END DRIVING

    # TRANSIT
    transit = client.distance_matrix(
        origins=[
            (enriched_points[0].get("latitude"), enriched_points[0].get("longitude"))
        ],
        destinations=[
            (enriched_points[1].get("latitude"), enriched_points[1].get("longitude"))
        ],
        mode="transit",
        units="metric",
        departure_time=now,
    )
    transit_duration_in_s = (
        transit.get("rows")[0].get("elements")[0].get("duration").get("value")
    )
    transit_distance_in_m = (
        transit.get("rows")[0].get("elements")[0].get("distance").get("value")
    )
    # END TRANSIT

    # BICYCLING
    bicycling = client.distance_matrix(
        origins=[
            (enriched_points[0].get("latitude"), enriched_points[0].get("longitude"))
        ],
        destinations=[
            (enriched_points[1].get("latitude"), enriched_points[1].get("longitude"))
        ],
        mode="bicycling",
        units="metric",
        departure_time=now,
    )
    bicycling_duration_in_s = (
        bicycling.get("rows")[0].get("elements")[0].get("duration").get("value")
    )
    bicycling_distance_in_m = (
        bicycling.get("rows")[0].get("elements")[0].get("distance").get("value")
    )
    # END BICYCLING

    row = {
        "id_origin": id_origin,
        "id_destination": id_destination,
        "driving_duration_in_s": driving_duration_in_s,
        "transit_duration_in_s": transit_duration_in_s,
        "bicycling_duration_in_s": bicycling_duration_in_s,
        "driving_distance_in_m": driving_distance_in_m,
        "transit_distance_in_m": transit_distance_in_m,
        "bicycling_distance_in_m": bicycling_distance_in_m,
    }
    return row


def write_to_bigquery(client, table_id, data):
    response = client.insert_rows_json(table_id, data)
    if response == []:
        pass
    else:
        logging.error(
            f"Encountered error while inserting rows to table {table_id}:\n {data}"
        )


def hello_pubsub(event, context):
    """
    Function that loads city boundary geojson files
    and for each samples samples random points (distributed according to mobile data).
    Then request google maps directions api for time from point A to B for various
    means of travel. Save output to database.
    """
    bucket_name = "bucket-city-population-grids"
    bigquery_distance_table_id = "journey_durations"
    bigquery_meta_table_id = "journey_metadata"
    maps_api_key = os.environ["GOOGLE_MAPS_API_KEY"]

    gcs_client = storage.Client()
    bucket_city_grids = gcs_client.bucket(bucket_name)
    city_grids = list_blobs(client=gcs_client, bucket_name=bucket_name)

    bigquery_client = bigquery.Client()
    gmaps = googlemaps.Client(key=maps_api_key)

    distance_table_staging = []
    meta_table_staging = []

    for grid_file in city_grids:
        points = generate_weighted_pairs_of_points(
            bucket_city_grids=bucket_city_grids, grid_file=grid_file
        )

        enriched_points = [enrich_point(p) for p in points]

        insert_meta_table, id_origin, id_destination = generate_meta_table_rows(
            enriched_points=enriched_points
        )
        meta_table_staging.extend(insert_meta_table)

        distance_table_row = generate_distance_table_rows(
            id_origin=id_origin,
            id_destination=id_destination,
            client=gmaps,
            enriched_points=enriched_points,
        )
        distance_table_staging.append(distance_table_row)

    write_to_bigquery(
        client=bigquery_client,
        table_id=bigquery_distance_table_id,
        data=distance_table_staging,
    )

    write_to_bigquery(
        client=bigquery_client,
        table_id=bigquery_meta_table_id,
        data=meta_table_staging,
    )
