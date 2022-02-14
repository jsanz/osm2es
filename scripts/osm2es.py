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

ES_URL = os.getenv("ES_URL")
TASK_NAME = os.getenv("TASK_NAME")
ES_INDEX_PREFFIX = os.getenv("ES_INDEX_PREFFIX", "openstreetmap")

if not (ES_URL and TASK_NAME and ES_INDEX_PREFFIX):
    print("ES_URL and/or TASK_NAME not defined!!")
    sys.exit(1)

DATA_FILE = f"./data/{TASK_NAME}/data.pbf"
INDEX_NAME = f"{ES_INDEX_PREFFIX}_{TASK_NAME}"

geojson = geom.GeoJSONFactory()

logger = logging.getLogger(__name__)

logging.getLogger("elastic_transport").setLevel(logging.WARNING)


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
            settings={"number_of_shards": 1, "number_of_replicas": 0},
            mappings={
                "properties": {
                    # common
                    "osm_id": {"type": "keyword"},
                    "osm_version": {"type": "integer"},
                    "osm_type": {"type": "keyword"},
                    "visible": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "point": {"type": "geo_point"},
                    "nodes": {"type": "keyword"},
                    "geometry": {"type": "geo_shape"},
                    "other_tags": {"type": "flattened"},
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

    def node(self, node):
        """
        Import OSM node into database as node

        Arguments:
            node {Node} -- osmium node object
        """
        if node.location.valid():

            node_db = {
                "osm_id": node.id,
                "osm_version": node.version,
                "osm_type": "node",
                "visible": node.visible,
                "timestamp": node.timestamp,
                "other_tags": self.tags2dict(tags=node.tags, type="node"),
            }

            node_db["point"] = [node.location.lon, node.location.lat]

            for prop in self.OSM_TAGS["node"]:
                if prop in node.tags:
                    node_db[prop] = node.tags[prop]

            self.cache.append(node_db)

            self.increment_cache("node")

    def way(self, way):
        """
        Import OSM way into database as way

        Arguments:
            way {Way} -- osmium way object
        """
        nodes = []
        for node in way.nodes:
            nodes.append(node.ref)

        way_db = {
            "osm_id": way.id,
            "osm_version": way.version,
            "osm_type": "way",
            "visible": way.visible,
            "timestamp": way.timestamp,
            "nodes": nodes,
            "geometry": json.loads(geojson.create_linestring(way)),
            "other_tags": self.tags2dict(tags=way.tags, type="way"),
        }

        for prop in self.OSM_TAGS["way"]:
            if prop in way.tags:
                way_db[prop] = way.tags[prop]

        self.cache.append(way_db)

        self.increment_cache("way")

    def relation(self, rel):
        """
        Import OSM relation into database as relation

        Arguments:
            rel {Relation} -- osmium relation object
        """

        rel_db = {
            "osm_id": rel.id,
            "osm_version": rel.version,
            "osm_type": "relation",
            "visible": rel.visible,
            "timestamp": rel.timestamp,
            "members": self.members2dict(rel.members),
            "other_tags": self.tags2dict(tags=rel.tags, type="relation"),
        }

        for prop in self.OSM_TAGS["relation"]:
            if prop in rel.tags:
                rel_db[prop] = rel.tags[prop]

        self.cache.append(rel_db)

        self.increment_cache("rel")

    def area(self, area):

        area_db = {
            "osm_id": area.id,
            "osm_version": area.version,
            "osm_type": "area",
            "visible": area.visible,
            "timestamp": area.timestamp,
            "geometry": json.loads(geojson.create_multipolygon(area)),
            "other_tags": self.tags2dict(tags=area.tags, type="area"),
        }

        for prop in self.OSM_TAGS["area"]:
            if prop in area.tags:
                area_db[prop] = area.tags[prop]

        self.cache.append(area_db)

        self.increment_cache("area")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = Elasticsearch(ES_URL)

    osmHandler = OSMtoESHandler(db_cache_size=5000, esclient=client)
    logger.info(f"Creating index [{INDEX_NAME}]...")
    osmHandler.create_index()
    logger.info("import {}".format("andorra-latest.osm.pbf"))
    osmHandler.show_import_status()

    cache_system = "flex_mem"
    osmHandler.apply_file(filename=DATA_FILE, locations=True, idx=cache_system)
    osmHandler.show_import_status()
    osmHandler.save_cache()
    logger.info("import done")
