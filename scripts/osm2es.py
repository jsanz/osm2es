import sys
import os
import logging
import json
from collections import namedtuple, Counter, defaultdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from osmium import SimpleHandler
from osmium import geom
from osmium.osm import RelationMember

from dotenv import load_dotenv

load_dotenv()

TASK_NAME = os.getenv("TASK_NAME")
ES_URL = os.getenv("ES_URL")
ES_INDEX_PREFFIX = os.getenv("ES_INDEX_PREFFIX", "openstreetmap")
ES_REPLICAS = int(os.getenv("ES_REPLICAS", "0"))

if not (ES_URL and TASK_NAME):
    print("ES_URL and/or TASK_NAME not defined!!")
    sys.exit(1)

DATA_FILE = f"./data/{TASK_NAME}/data.pbf"
INDEX_NAME = f"{ES_INDEX_PREFFIX}_{TASK_NAME}"

geojson = geom.GeoJSONFactory()

logger = logging.getLogger(__name__)

logging.getLogger("elastic_transport").setLevel(logging.WARNING)


# TODO: Implement a Queue and multiprocessing like in 
# https://github.com/Sophox/sophox/blob/main/osm2rdf/RdfFileHandler.py
class OSMtoESHandler(SimpleHandler):
    OSM_TAGS = {
        "node": [
            "name",
            "man_made",
            "wikidata",
            "highway",
            "address",
            "amenity",
            "crossing",
            "entrance",
            "leisure",
            "natural",
            "office",
            "place",
            "shop",
            "wheelchair",
        ],
        "way": [
            "name",
            "man_made",
            "wikidata",
            "highway",
            "access",
            "aerialway",
            "barrier",
            "cycleway",
            "lanes",
            "layer",
            "junction",
            "maxspeed",
            "network",
            "oneway",
            "ref",
            "route",
            "surface",
            "waterway",
        ],
        "area": [
            "name",
            "natural",
            "man_made",
            "wikidata",
            "admin_level",
            "boundary",
            "landuse",
            "building",
        ],
        "relation": ["name", "man_made", "wikidata"],
    }

    def __init__(self, db_cache_size, esclient):
        SimpleHandler.__init__(self)

        self.cache = []

        self.counter = Counter(
            {
                "node": 0,
                "way": 0,
                "rel": 0,
                "area": 0,
            }
        )

        self.db_cache_size = db_cache_size
        self.esclient = esclient
        self.index_name = INDEX_NAME

    def create_index(self):
        if self.esclient.indices.exists(index=self.index_name):
            logger.info("Index {} exists. Deleting...".format(self.index_name))
            self.esclient.indices.delete(index=self.index_name)
        self.esclient.indices.create(
            index=self.index_name,
            timeout="60s",
            settings={"number_of_shards": 1, "number_of_replicas": ES_REPLICAS},
            mappings={
                "properties": {
                    # common
                    "osm_id": {"type": "keyword"},
                    "osm_version": {"type": "integer"},
                    "osm_type": {"type": "keyword"},
                    "osm_user": {"type": "keyword"},
                    "visible": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "nodes": {"type": "keyword"},
                    "point": {"type": "geo_point"},
                    "geometry": {"type": "geo_shape"},
                    "other_tags": {"type": "flattened"},
                    "num_tags": {"type": "integer"},
                    "name": {"type": "text"},
                    "man_made": {"type": "keyword"},
                    "wikidata": {"type": "text"},
                    "highway": {"type": "keyword"},
                    "members": {
                        "properties": {
                            "ref": {"type": "keyword"},
                            "role": {"type": "keyword"},
                            "type": {"type": "keyword"},
                        }
                    },
                    # node
                    "address": {"type": "text"},
                    "amenity": {"type": "keyword"},
                    "crossing": {"type": "keyword"},
                    "entrance": {"type": "keyword"},
                    "leisure": {"type": "keyword"},
                    "natural": {"type": "keyword"},
                    "office": {"type": "keyword"},
                    "place": {"type": "keyword"},
                    "shop": {"type": "keyword"},
                    "wheelchair": {"type": "keyword"},
                    # way
                    "access": {"type": "keyword"},
                    "aerialway": {"type": "keyword"},
                    "barrier": {"type": "keyword"},
                    "cycleway": {"type": "keyword"},
                    "lanes": {"type": "keyword"},
                    "layer": {"type": "keyword"},
                    "junction": {"type": "keyword"},
                    "maxspeed": {"type": "keyword"},
                    "network": {"type": "keyword"},
                    "oneway": {"type": "keyword"},
                    "ref": {"type": "text"},
                    "route": {"type": "keyword"},
                    "surface": {"type": "keyword"},
                    "waterway": {"type": "keyword"},
                    # area
                    "admin_level": {"type": "keyword"},
                    "boundary": {"type": "keyword"},
                    "building": {"type": "keyword"},
                    "landuse": {"type": "keyword"},
                    "natural": {"type": "keyword"},
                }
            },
        )

    def show_import_status(self):
        """
        Show import status for every 10000 objects
        """
        if sum(self.counter.values()) % 10000 == 0:
            logger.info(
                "Nodes {node:d} | Ways {way:d} | Rel {rel:d} | Area {area:d}".format(
                    **self.counter
                )
            )

    def check_cache_save(self):
        """
        Check if chunk size is full and import cached objects
        """
        if len(self.cache) % self.db_cache_size == 0:
            self.save_cache()

    def increment_cache(self, type):
        self.counter[type] += 1
        self.check_cache_save()
        self.show_import_status()

    def save_cache(self):
        """
        Save cached objects into Elasticsearch and clear cache
        """
        actions, errs = bulk(
            client=self.esclient, index=self.index_name, actions=self.cache
        )
        for err in errs:
            logger.error(err)
        logger.debug("Imported {} items".format(actions))
        self.cache.clear()

    def members2dict(self, members):
        member_list = []
        for member in members:
            if isinstance(member, tuple):
                m = namedtuple("member", ("type", "ref", "role"))
            elif isinstance(member, RelationMember):
                m = member
            member_list.append({"ref": m.ref, "role": m.role, "type": m.type})
        return member_list

    def tags2dict(self, tags, type):
        """
        Convert osmium TagList into python dict

        Arguments:
            tags {TagList} -- osmium TagList for a geo-object

        Returns:
            dict -- tags in a python dict
        """
        tag_dict = {}

        for tag in tags:
            if tag.k not in self.OSM_TAGS[type]:
                tag_dict[tag.k] = tag.v

        return tag_dict

    def process_element(self, element, geometry, type, base_db = {}):
        """
        Process a OSM object

        Arguments:
            element -- osmium OSM oject
            geometry -- a geometry constructed from the OSM object
            type -- node|way|area|rel
            based_db -- a preprocessed object to update
        """
        element_db = {
            "osm_id": element.id,
            "osm_version": element.version,
            "osm_user": element.user,
            "visible": element.visible,
            "timestamp": element.timestamp,
            "osm_type": type,
            "num_tags": len(element.tags),
            "other_tags": self.tags2dict(tags=element.tags, type=type)
        }

        element_db.update(base_db)

        if geometry:
            element_db["geometry"] = json.loads(geometry)        

        for prop in self.OSM_TAGS[type]:
            if prop in element.tags:
                element_db[prop] = element.tags[prop]

        self.cache.append(element_db)

        self.increment_cache(type)

    def node(self, node):
        """
        Import OSM node into database as node

        Arguments:
            node {Node} -- osmium node object
        """
        try:
            if node.visible and node.location.valid():
                geometry = geojson.create_point(node)
                base_db = {
                    "point": [node.location.lon, node.location.lat]
                }
                self.process_element(node, geometry, "node", base_db=base_db)
        except Exception as ex:
            logger.error(f"There was an error loading node {node.id}: {ex}")

    def way(self, way):
        """
        Import OSM way into database as way

        Arguments:
            way {Way} -- osmium way object
        """
        try:
            if not way.visible:
                return
            geometry = geojson.create_linestring(way)
            self.process_element(way, geometry, "way")
        except Exception as ex:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logger.error(f"There was an error loading way {way.id}: {exc_type}")

    def relation(self, rel):
        pass

    def area(self, area):
        try:
            if not area.visible:
                return
            
            geometry = geojson.create_multipolygon(area)
            self.process_element(area, geometry, "area")
        except Exception as ex:
            logger.error(f"There was an error loading area {area.id}: {ex}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = Elasticsearch(ES_URL)

    osmHandler = OSMtoESHandler(db_cache_size=5000, esclient=client)
    logger.info(f"Creating index [{INDEX_NAME}]...")
    osmHandler.create_index()
    logger.info(f"import {DATA_FILE}...")
    osmHandler.show_import_status()

    cache_system = "flex_mem"
    osmHandler.apply_file(filename=DATA_FILE, locations=True, idx=cache_system)
    osmHandler.show_import_status()
    osmHandler.save_cache()
    logger.info("import done")
