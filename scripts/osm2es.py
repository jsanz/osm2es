import os
from collections import namedtuple, Counter, defaultdict
import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from osmium import SimpleHandler
from osmium import geom
from osmium.osm import RelationMember

ES_URL=os.getenv('ES_URL','http://localhost:9200')
DATA_FILE=os.getenv('DATA_FILE','.data/andorra/data.pbf')

geojson = geom.GeoJSONFactory()

logger = logging.getLogger(__name__)


class OSMtoESHandler(SimpleHandler):
    def __init__(self, db_cache_size, esclient):
        SimpleHandler.__init__(self)

        self.cache = []

        self.counter = Counter({
            'node': 0,
            'way': 0,
            'rel': 0,
            'area': 0,
        })

        self.db_cache_size = db_cache_size
        self.esclient = esclient
        self.index_name = 'openstreetmap'

    def create_index(self):
        if self.esclient.indices.exists(index=self.index_name):
            logger.warning(
                'Index {} exists. Deleting...'.format(self.index_name))
            self.esclient.indices.delete(index=self.index_name)
        self.esclient.indices.create(
            index=self.index_name,
            timeout="60s",
            settings={"number_of_shards": 1},
            mappings={
                "properties": {
                    "osm_id": {"type": "keyword"},
                    "osm_version": {"type": "integer"},
                    "osm_type": { "type": "keyword"},
                    "visible": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "point": {"type": "geo_point"},
                    "tags": {"type": "flattened"},
                    "nodes": {"type": "keyword"},
                    "geometry": {"type": "geo_shape"},
                    "members": {
                        "properties": {
                            "ref": {"type": "keyword"},
                            "role": {"type": "keyword"},
                            "type": {"type": "keyword"}
                        }
                    }
                }
            }
        )

    def show_import_status(self):
        """
        Show import status for every 10000 objects
        """
        if sum(self.counter.values()) % 10000 == 0:
            logger.info(
                "Nodes {node:d} | Ways {way:d} | Rel {rel:d} | Area {area:d}"
                .format(**self.counter)
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
        actions, errs = bulk(client=self.esclient, index=self.index_name, actions=self.cache)
        for err in errs:
            logger.error(err)
        logger.debug("Imported {} items".format(actions))
        self.cache.clear()

    def members2dict(self, members):
        member_list = []
        for member in members:
            if isinstance(member, tuple):
                m = namedtuple('member', ('type', 'ref', 'role'))
            elif isinstance(member, RelationMember):
                m = member
            member_list.append({'ref': m.ref, 'role': m.role, 'type': m.type})
        return member_list

    def tags2dict(self, tags):
        """
        Convert osmium TagList into python dict

        Arguments:
            tags {TagList} -- osmium TagList for a geo-object

        Returns:
            dict -- tags in a python dict
        """
        tag_dict = {}

        for tag in tags:
            tag_dict[tag.k] = tag.v

        return tag_dict

    def node(self, node):
        """
        Import OSM node into database as node

        Arguments:
            node {Node} -- osmium node object
        """
        node_db = {
            "osm_id": node.id,
            "osm_version": node.version,
            "osm_type": "node",
            "visible": node.visible,
            "timestamp": node.timestamp,
            "tags": self.tags2dict(tags=node.tags)
        }

        if node.location.valid():
            node_db["point"] = [node.location.lon, node.location.lat]

        self.cache.append(node_db)

        self.increment_cache('node')

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
            "tags": self.tags2dict(tags=way.tags)
        }

        self.cache.append(way_db)

        self.increment_cache('way')

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
            "tags": self.tags2dict(tags=rel.tags)
        }

        self.cache.append(rel_db)

        self.increment_cache('rel')

    def area(self, area):

        area_db = {
            "osm_id": area.id,
            "osm_version": area.version,
            "osm_type": "area",
            "visible": area.visible,
            "timestamp": area.timestamp,
            "geometry": json.loads(geojson.create_multipolygon(area)),
            "tags": self.tags2dict(tags=area.tags)
        }

        self.cache.append(area_db)

        self.increment_cache('area')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    client = Elasticsearch(ES_URL)

    osmHandler = OSMtoESHandler(db_cache_size=500, esclient=client)
    logger.info("creating index")
    osmHandler.create_index()
    logger.info("import {}".format("andorra-latest.osm.pbf"))
    osmHandler.show_import_status()

    cache_system = "flex_mem"
    osmHandler.apply_file(
        filename=DATA_FILE, locations=True, idx=cache_system)
    osmHandler.show_import_status()
    osmHandler.save_cache()
    logger.info("import done")
