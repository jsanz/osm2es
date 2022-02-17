# OSM to Elasticsearch

## Set up

Create a virtual environment and install the libraries with `pip install -r requirements.txt`

## Run

```text
$ python scripts/osm2es.py --help
usage: python3 osm2es.py

Imports OSM data into Elasticsearch

positional arguments:
  input_file            OSM input PBF file

optional arguments:
  -h, --help            show this help message and exit
  --index-name INDEX_NAME
                        Index name
  --es-url ES_URL       Elasticsearch url (default: http://localhost:9200)
  --es-user ES_USER     Elasticsearch user (default: elastic)
  --es-pwd ES_PWD       Elasticsearch password (default: changeme)
  --es-replicas ES_REPLICAS
                        Index replicas (default: 0)
  --workers WORKER_COUNT
                        Number of worker threads to run (default: 1)
  --cache-size DB_CACHE_SIZE
                        Number of documents to accumulate before sending to ES (default: 5000)
  -v                    Enable verbose output.
```

**Notes**:

* The script will overwrite the index passed so be sure you are OK with that
* By default it will use a single worker in parallel with the data read. You may want to try but 6 to 8 workers should work best
