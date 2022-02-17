import logging
import argparse
import sys

from handler import OSMtoESHandler


logger = logging.getLogger(__name__)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)


def parse_fail(parser, info):
    print(info)
    parser.print_help()
    exit(1)


if __name__ == "__main__":

    # create the parser
    parser = argparse.ArgumentParser(
        description="Imports OSM data into Elasticsearch", usage="python3 %(prog)s"
    )

    parser.add_argument("input_file", help="OSM input PBF file")
    parser.add_argument(
        "--index-name",
        action="store",
        dest="index_name",
        default="openstreetmap",
        help="Index name",
    )

    parser.add_argument(
        "--es-url",
        action="store",
        dest="es_url",
        default="http://localhost:9200",
        help="Elasticsearch url (default: %(default)s)",
    )
    parser.add_argument(
        "--es-user",
        action="store",
        dest="es_user",
        default="elastic",
        help="Elasticsearch user (default: %(default)s)",
    )
    parser.add_argument(
        "--es-pwd",
        action="store",
        dest="es_pwd",
        default="changeme",
        help="Elasticsearch password (default: %(default)s)",
    )
    parser.add_argument(
        "--es-replicas",
        action="store",
        dest="es_replicas",
        default=0,
        type=int,
        help="Index replicas (default: %(default)s)",
    )

    parser.add_argument(
        "--workers",
        action="store",
        dest="worker_count",
        default=1,
        type=int,
        help="Number of worker threads to run (default: %(default)s)",
    )
    parser.add_argument(
        "--cache-size",
        action="store",
        dest="db_cache_size",
        default=5000,
        type=int,
        help="Number of documents to accumulate before sending to ES (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        action="store_true",
        dest="verbose",
        default=False,
        help="Enable verbose output.",
    )
    opts = parser.parse_args()

    if not opts.input_file:
        parse_fail(parser, "Missing input file")

    logging_level = logging.DEBUG if opts.verbose else logging.INFO
    logging.basicConfig(
        format='%(asctime)s %(name)-8s %(processName)-13s %(levelname)-8s %(message)s',
        datefmt='%H:%M:%S',
        level=logging_level)

    try:
        logger.info("Starting import process")
        with OSMtoESHandler(opts) as handler:
            handler.run(opts.input_file)
            logger.info("Import done")
    except KeyboardInterrupt:
        logger.warning("Finshing by keyboard")
        sys.exit(-1)