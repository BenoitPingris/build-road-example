from typing import Union, Optional, cast 
from dataclasses import dataclass
from pprint import pprint
import bson
import pymongo
import pandas
import geopandas
import shapely.geometry
import shapely.ops
import geopandas.geoseries

def read_file():
    return geopandas.read_file("./shapefile/gis_osm_roads_free_1.shp")

def chunker(df: Union[pandas.DataFrame, geopandas.GeoDataFrame], chunksize: int):
    for i in range(0, len(df), chunksize):
        yield df[i:i+chunksize]


def migrate(client: pymongo.MongoClient):
    print("[!] dropping roads collection...")
    client.lycos.roads.drop()
    print("[✓] done.")

    print("[!] reading shapefile...")
    data = read_file()
    print("[✓] done.")
    batch = []
    batchsize = 5000
    print(f"[!] inserting the data by batch of {batchsize}...")
    for i, row in data.iterrows():
        i = cast(int, i)

        batch.append({
            "location": {
                "type": "LineString", 
                "coordinates": [c for c in row["geometry"].coords] #type: ignore
                },
            "name": row["name"]
            })
        if i % batchsize == 0:
            print(f"[!] inserting batch #{i//batchsize}")
            client.lycos.roads.insert_many(batch)
            batch = []

    if len(batch) > 0:
        print(f"[!] inserting final batch")
        client.lycos.roads.insert_many(batch)
    print("[✓] done.")
    print("[!] creating 2dsphere index...")
    client.lycos.roads.create_index([("location", pymongo.GEOSPHERE)])
    print("[✓] done.")
    
@dataclass
class Location:
    lat: float
    lon: float

def find_first_segment(client: pymongo.MongoClient, loc: Location):
    road = client.lycos.roads.find_one({
        "location": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [loc.lon, loc.lat]
                    }
                }
            }
        })
    return road

def find_segment(client: pymongo.MongoClient, loc: Location, name: str, ids: list[bson.ObjectId] = []):
    results = client.lycos.roads.find_one({
        "location": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [loc.lon, loc.lat]
                    }
                }
            },
        "_id": {
            "$nin": ids
        },
        "name": name
    })
    return results

def tojson(s):
    print(geopandas.GeoSeries([s]).to_json())

def build_road(client: pymongo.MongoClient, starter_point: Location):
    starter_road = find_first_segment(client, starter_point)
    if not starter_road:
        print("no road found :(")
        return

    processed_roads: list[bson.ObjectId] = [starter_road["_id"]]
    road = shapely.geometry.LineString(starter_road["location"]["coordinates"])

    extremities: list[Location] = [Location(*reversed(road.coords[0])), Location(*reversed(road.coords[-1]))]
    name = starter_road["name"]

    last_len = len(road.coords)
    while True:
        for ex in extremities:
            segment = find_segment(client, ex, name, processed_roads)

            if not segment:
                continue
            segment_coords = segment["location"]["coordinates"]
            tmp_line = shapely.geometry.LineString(segment_coords)
            tmp_multilinestring: Optional[shapely.geometry.MultiLineString] = None

            if list(road.coords[0]) in [segment_coords[0], segment_coords[-1]]:
                print("left")
                tmp_multilinestring = shapely.geometry.MultiLineString([cast(shapely.geometry.LineString, road), tmp_line])
            elif list(road.coords[-1]) in [segment_coords[0], segment_coords[-1]]:
                print("right")
                tmp_multilinestring = shapely.geometry.MultiLineString([tmp_line, cast(shapely.geometry.LineString, road)])

            road = shapely.ops.linemerge(tmp_multilinestring)
            processed_roads.append(segment["_id"])

        if last_len == len(road.coords):
            print("done.")
            break
        last_len = len(road.coords)
        extremities: list[Location] = [Location(*reversed(road.coords[0])), Location(*reversed(road.coords[-1]))]

    print(tojson(road))



def main():
    client = pymongo.MongoClient(host="localhost", port=27017, username="lycos", password="lycos")
    # migrate(client)
    starter_point = Location(48.9545232, 4.3642834)
    build_road(client, starter_point)
    return


if __name__ == "__main__":
    main()
