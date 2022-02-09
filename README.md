# OSM to Elasticsearch

## Requirements

* Docker 
* Docker Compose

## How to run

### Set up

Copy the `.env.sample`:

* Set up your Elasticsearch host and credentials
* Define the area to download

You can run the following command to check for example the areas available by `geofabrik`:

```
docker run --rm  openmaptiles/openmaptiles-tools download-osm list geofabrik 
```

Check [`download-osm`](https://github.com/openmaptiles/openmaptiles-tools/blob/master/bin/download-osm) for more details on how this tool works.


### Download

Run the download process with:

```sh
docker-compose up download
```

It should create a `data/data.pbf` file with the area selected.

### Upload

Run the upload process with: 

```sh
docker-compose up upload
```

The upload process will rotate over the different layers of the `pbf` file and generate the following indexes, assuming `ES_INDEX=osm`

* `osm_points`
* `osm_lines`
* `osm_multilines`
* `osm_multipolygons`
* `osm_other_relations`

You can create then a Data View in Kibana pointing to `osm*` and then use filtering capabilities in Elastic Maps to decide what do you want to render.

