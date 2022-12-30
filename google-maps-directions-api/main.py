import sys
from datetime import datetime, timedelta
from io import StringIO
from random import random
import os
import google.cloud.logging

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
    try:
        data_str = bucket.blob(file).download_as_text()
    except Exception:
        logging.error(
            f"Encountered an error while loading city grid from bucket: {bucket}, and file: {file}."
        )
        sys.exit()
    return pd.read_csv(StringIO(data_str))


def generate_weighted_pairs_of_points(bucket_city_grids, grid_file):

    df = load_city_grid(bucket=bucket_city_grids, file=grid_file)
    sample = df.sample(n=2, weights="density")
    points = [
        generate_point_in_neighborhood(
            center=Point(row["latitude"], row["longitude"]), radius=500
        )
        for _, row in sample.iterrows()
    ]
    city_name = df.iloc[0]["city"]
    return points, city_name


def enrich_point(point: Point, city: str) -> dict:
    res = {}
    res["latitude"], res["longitude"], altitude = point

    query = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={res['latitude']}&lon={res['longitude']}"

    try:
        r = requests.get(query)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(e.response.text)
        sys.exit()

    geodata = r.json()

    if geodata.get("address") is not None:
        address = geodata.get("address")
        res["house_number"] = address.get("house_number")
        res["road"] = address.get("road")
        res["neighbourhood"] = address.get("neighbourhood")
        res["suburb"] = address.get("suburb")
        res["city_district"] = address.get("city_district")
        res["city"] = city
        res["state"] = address.get("state")
        res["postcode"] = address.get("postcode")
        res["country"] = address.get("country")
        res["country_code"] = address.get("country_code")

    else:
        logging.error(f"Encountered an error while retrieving address of {geodata}.")
        sys.exit()

    return res


def generate_stop_rows(enriched_points: list[dict]):
    rows = []

    insertion_time_origin = datetime.now()
    insertion_time_destination = insertion_time_origin + timedelta(seconds=1)

    id_origin = int(round(datetime.timestamp(insertion_time_origin)))
    id_destination = int(round(datetime.timestamp(insertion_time_destination)))

    origin = {
        "id": id_origin,
        "insertion_time": insertion_time_origin.strftime(format="%Y-%m-%d %H:%M:%S.%f"),
        "type": "origin",
    }
    destination = {
        "id": id_destination,
        "insertion_time": insertion_time_destination.strftime(
            format="%Y-%m-%d %H:%M:%S.%f"
        ),
        "type": "destination",
    }

    origin.update(enriched_points[0])
    destination.update(enriched_points[1])

    rows.append(origin)
    rows.append(destination)

    return rows, id_origin, id_destination


def generate_journey_rows(
    id_origin: int,
    id_destination: int,
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
        "origin": id_origin,
        "destination": id_destination,
        "insertion_time": now.strftime(format="%Y-%m-%d %H:%M:%S.%f"),
        "driving_duration": driving_duration_in_s,
        "transit_duration": transit_duration_in_s,
        "bicycling_duration": bicycling_duration_in_s,
        "driving_distance": driving_distance_in_m,
        "transit_distance": transit_distance_in_m,
        "bicycling_distance": bicycling_distance_in_m,
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


def journey(event, context):
    """
    Function that loads city boundary geojson files
    and for each samples samples random points (distributed according to mobile data).
    Then request google maps directions api for time from point A to B for various
    means of travel. Save output to database.
    """
    # setup logging
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

    bucket_name = "bucket-city-population-grids"
    bigquery_journeys_table_id = "tokyo-house-366821.urban_transport_monitor.journeys"
    bigquery_stops_table_id = "tokyo-house-366821.urban_transport_monitor.stops"
    maps_api_key = os.environ["GOOGLE_MAPS_API_KEY"]

    gcs_client = storage.Client()
    bucket_city_grids = gcs_client.bucket(bucket_name)
    city_grids = list_blobs(client=gcs_client, bucket_name=bucket_name)

    bigquery_client = bigquery.Client()
    gmaps = googlemaps.Client(key=maps_api_key)

    journeys_table_staging = []
    stops_table_staging = []

    for grid_file in city_grids:
        points, city_name = generate_weighted_pairs_of_points(
            bucket_city_grids=bucket_city_grids, grid_file=grid_file
        )

        enriched_points = [enrich_point(point=p, city=city_name) for p in points]

        stop_rows, id_origin, id_destination = generate_stop_rows(
            enriched_points=enriched_points
        )
        stops_table_staging.extend(stop_rows)

        journey_rows = generate_journey_rows(
            id_origin=id_origin,
            id_destination=id_destination,
            client=gmaps,
            enriched_points=enriched_points,
        )
        journeys_table_staging.append(journey_rows)

    write_to_bigquery(
        client=bigquery_client,
        table_id=bigquery_journeys_table_id,
        data=journeys_table_staging,
    )

    write_to_bigquery(
        client=bigquery_client,
        table_id=bigquery_stops_table_id,
        data=stops_table_staging,
    )
