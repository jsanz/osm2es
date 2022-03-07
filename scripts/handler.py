from collections import Counter
import traceback

from datetime import datetime
from operator import attrgetter

import logging
import json

from multiprocessing import Process, Queue

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from osmium import SimpleHandler
from osmium import geom

geojson = geom.GeoJSONFactory()

from tags import tags2dict, OSM_TAGS, INDEX_MAPPINGS

logger = logging.getLogger("handler")
logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_client(url, user, password):
    """
    Returns an Elasticsearch client

    Arguments:
        - url - Elasticsearh URL
        - user - Elasticsearch user name
        - password - Elasticsearch password
    """
    return Elasticsearch(url, http_auth=(user, password))


def writer_thread(worker_id, queue, es_url, es_user, es_pwd, index_name):
    """
    This function will run in a forked process, receiving the Elasticsearch
    info to create the client and a reference to the Queue to get data from
    
    Arguments:
        obj {Area} -- osmium area object
    """
    logger.info(f"Starting worker: {worker_id}")
    client = get_client(es_url, es_user, es_pwd)
    indexed_docs = 0

    while True:
        ts, job_counter, data = queue.get()
        if ts is None:
            logger.info(f"Writer {worker_id} indexed {indexed_docs} documents")
            return
        
        indexed_docs += write_actions(client, index_name, data)


def write_actions(client, index_name, data):
    """
    Save cached objects into Elasticsearch
    """
    #actions = list(map(lambda item: get_action(index_name, item, item["osm_id"]), data))
    try:
        actions, errs = bulk(client=client, index=index_name, actions=data)
        logger.debug(f"{actions} documents indexed")

        if len(errs) > 0:
            logger.info(f"{len(errs)} documents failed to index")
        
        return 0 if actions is None else actions
    except:
        logger.error("An exception triggered on uploading data to ES")
        return 0

class OSMtoESHandler(SimpleHandler):
    def __init__(self, opts):
        SimpleHandler.__init__(self)

        self.options = opts
        self.db_cache_size = opts.db_cache_size

        self.job_counter = 1
        self.pending = []
        self.pendingCount = 0


        self.counter = Counter(
            {
                "node": 0,
                "way": 0,
                "rel": 0,
                "area": 0,
            }
        )

        try:
            self.create_index()
        except:
            raise ValueError("Error creating the ES index, check URL and credentials")

        # Queue should contain at most 1 item, making the total number of batches in memory to be
        # number_of_workers + one_in_query + one_being_assembled_by_main_thread
        self.queue = Queue(1)

        self.writers = []

        index_name, es_url, es_user, es_pwd = attrgetter(
            "index_name", "es_url", "es_user", "es_pwd"
        )(self.options)

        for worker_id in range(opts.worker_count):
            process = Process(
                target=writer_thread,
                args=(worker_id, self.queue, es_url, es_user, es_pwd, index_name),
            )
            self.writers.append(process)
            process.start()

    def __enter__(self):
        """
        Context manager enter method
        """
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """
        Context manager exit method
        """
        self.flush()

    def show_import_status(self):
        """
        Show import status for every 50000 objects
        """
        if sum(self.counter.values()) % 50000 == 0:
            logger.info(
                "PBF data read: Nodes {node:d} | Ways {way:d} | Rel {rel:d} | Area {area:d}".format(
                    **self.counter
                )
            )

    def create_index(self):
        """
        Uses the object options to create a new index, 
        removing it if it already exists 
        """
        index_name, es_url, es_user, es_pwd, es_replicas = attrgetter(
            "index_name", "es_url", "es_user", "es_pwd", "es_replicas"
        )(self.options)

        logger.info(f"Creating index [{index_name}]...")

        client = get_client(es_url, es_user, es_pwd)

        if client.indices.exists(index=index_name):
            logger.info("Index {} exists. Deleting...".format(index_name))
            client.indices.delete(index=index_name)
        client.indices.create(
            index=index_name,
            timeout="60s",
            settings={"number_of_shards": 1, "number_of_replicas": es_replicas},
            mappings=INDEX_MAPPINGS,
        )

    def process_element(self, element, geometry, type, base_db={}):
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
            "other_tags": tags2dict(tags=element.tags, type=type),
        }

        element_db.update(base_db)

        if geometry:
            element_db["geometry"] = json.loads(geometry)

        for prop in OSM_TAGS[type]:
            if prop in element.tags:
                element_db[prop] = element.tags[prop]

        self.counter[type] += 1
        self.show_import_status()
        self.finalize_object(element_db)

    def node(self, obj):
        """
        Import OSM node into database as node

        Arguments:
            obj {Node} -- osmium node object
        """
        obj_type = "node"
        try:
            if obj.visible and obj.location.valid():
                geometry = geojson.create_point(obj)
                base_db = {"point": [obj.location.lon, obj.location.lat]}
                self.process_element(obj, geometry, obj_type, base_db=base_db)
        except:
            logger.error(f"There was an error loading {obj_type} {obj.id}")
            logger.error(traceback.format_exc())

    def way(self, obj):
        """
        Import OSM way into database as linestring

        Arguments:
            obj {Way} -- osmium way object
        """
        obj_type = "way"
        try:
            if not obj.visible:
                return
            geometry = geojson.create_linestring(obj)
            self.process_element(obj, geometry, obj_type)
        except:
            logger.error(f"There was an error loading {obj_type} {obj.id}")
            logger.error(traceback.format_exc())

    def relation(self, rel):
        """
        Import OSM relation is a no op
        """
        pass

    def area(self, obj):
        """
        Import OSM area into database as a multypolgyon

        Arguments:
            obj {Area} -- osmium area object
        """
        obj_type = "area"
        try:
            if not obj.visible:
                return

            geometry = geojson.create_multipolygon(obj)
            self.process_element(obj, geometry, "area")
        except:
            logger.error(f"There was an error loading {obj_type} {obj.id}")
            logger.error(traceback.format_exc())

    def finalize_object(self, obj):
        """
        Adds the object to the pending array,
        flushing the cache if necessary

        Arguments:
            obj -- any osmium OSM object
        """
        try:
            if obj:
                self.pending.append(obj)
                self.pendingCount += 1

                if self.pendingCount >= self.db_cache_size:
                    self.flush()
        except Exception as e:
            logger.error(f"Error finalizing object {e}")
            raise e


    def flush(self):
        """
        Adds the pending documents to the instance queue
        """
        if self.pendingCount == 0:
            return

        self.queue.put(
            (datetime.utcnow(), self.job_counter, self.pending)
        )

        self.job_counter += 1
        self.pending = []
        self.pendingCount = 0

    def run(self, input_file):
        """
        Starts the processing of the OSM data file.

        Arguments:
            input_file -- OSM pbf file
        """
        logger.info(f"Importing {input_file}...")

        cache_system = "flex_mem"
        self.apply_file(filename=input_file, locations=True, idx=cache_system)

        self.flush()

        # Send stop signal to each worker, and wait for all to stop
        for _ in self.writers:
            logger.debug(f"Stopping writer...")
            self.queue.put((None, None, None))
        
        logger.debug("Closing the Queue")
        self.queue.close()
        
        for p in self.writers:
            p.join()
        
        self.show_import_status()
        logger.info("Done!")
