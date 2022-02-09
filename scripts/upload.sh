#!/bin/bash

log()
{
    echo "[$(date --iso-8601=minutes)] $1"
}

upload_dataset()
{
    layer=$1
    index_name=${ES_INDEX}_${layer}

    # Deleting if index exists
    if $(curl -sI "${ES_URL}/${index_name}" | grep "HTTP/2 200" > /dev/null); then
        log "Deleting {${index_name}} exists..."
        curl -s -XDELETE "${ES_URL}/${index_name}"
        echo ""
    fi

    log "Uploading the data into the ${index_name} index..."
    ogr2ogr -progress -skipfailures\
        -lco INDEX_NAME=${index_name} \
        -lco MAPPING=/app/mappings/openstreetmap_${layer}.json \
        ES:${ES_URL} \
        ${DATA_FILE} ${layer}
    
    echo "-----------------------------"
}

DATA_FILE=/app/data/data.pbf

if [ ! -e ${DATA_FILE} ]; then
  echo "Data file {${DATA_FILE}} not found!"
  exit 1
fi

log "Starting process to upload data from ${DOWNLOAD_AREA}"

for layer in points lines multilinestrings multipolygons other_relations
do
    upload_dataset $layer
done

log "Done!"
