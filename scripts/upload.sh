#!/bin/bash

log()
{
    echo "[$(date --iso-8601=minutes)] $1"
}

convert_into_gpkg()
{
    layer=$1

    log "Converting the ${layer} data into the geopackage..."
    ogr2ogr -progress -skipfailures \
        -update -overwrite \
        -lco GEOMETRY_NAME=geometry \
        "${DATA_FILE}.gpkg" \
        ${DATA_FILE} ${layer}
}

delete_index()
{
    index_name=$1
    # Deleting if index exists
    if $(curl -sI "${ES_URL}/${index_name}" | grep -q "^HTTP.*200"); then
        curl -s -XDELETE "${ES_URL}/${index_name}"
        echo ""
        log "${index_name} deleted"
    fi
}

create_index()
{
    index_name=$1
    curl -sXPUT "${ES_URL}/${index_name}" \
        -H 'Content-Type: application/json' \
        -d "{\"settings\":{\"index\":{\"number_of_replicas\":${ES_REPLICAS}}},\"mappings\":{\"properties\":{\"geometry\":{\"type\":\"geo_shape\"},\"osm_id\":{\"type\":\"text\"},\"osm_version\":{\"type\":\"integer\"},\"osm_timestamp\":{\"type\":\"date\",\"format\":\"yyyy/MM/ddHH:mm:ss.SSS\"}}}}"
}

update_index_settings()
{
    index_name=$1
    curl -sXPUT "${ES_URL}/${index_name}/_settings" \
        -H 'Content-Type: application/json' \
        -d "{\"index\":{\"number_of_replicas\":${ES_REPLICAS}}},\"mappings\":{\"properties\":{\"geometry\":{\"type\":\"geo_shape\"},\"osm_id\":{\"type\":\"text\"},\"osm_version\":{\"type\":\"integer\"},\"osm_timestamp\":{\"type\":\"date\",\"format\":\"yyyy/MM/ddHH:mm:ss.SSS\"}}}"
}

report_index()
{
    index_name=$1
    echo "Number of documents in ${index_name}: $(curl -s "${ES_URL}/${index_name}/_count" | jq .count)"
}

# upload_dataset()
# {
#     layer=$1


#     log "Uploading the data into the ${index_name} index..."
#     ogr2ogr -progress -skipfailures\
#         -lco INDEX_NAME=${index_name} \
#         -lco MAPPING=/app/mappings/openstreetmap_${layer}.json \
#         ES:${ES_URL} \
#         ${DATA_FILE} ${layer}
#         #"${DATA_FILE}.gpkg" ${layer}
    
#     echo "-----------------------------"
# }

DATA_FILE=/app/data/${TASK_NAME}/data.pbf
LAYERS="points lines multilinestrings multipolygons other_relations"
# LAYERS="points multilinestrings other_relations"
# LAYERS="lines multipolygons"

if [ ! -e ${DATA_FILE} ]; then
  echo "Data file {${DATA_FILE}} not found!"
  exit 1
fi

log "Recreating indexes..."
for layer in ${LAYERS}
do
    index_name=${ES_INDEX_PREFFIX}_${TASK_NAME}_${layer}
    delete_index $index_name > /dev/null
done

log "Parallel uploading data from ${DOWNLOAD_AREA}"

parallel ogr2ogr -progress -skipfailures -overwrite \
        -lco INDEX_NAME=${ES_INDEX_PREFFIX}_${TASK_NAME}_{} \
        -lco MAPPING=/app/mappings/openstreetmap_{}.json \
        ES:${ES_URL} \
        ${DATA_FILE} {} ::: $LAYERS

for layer in ${LAYERS}
do
    index_name=${ES_INDEX_PREFFIX}_${TASK_NAME}_${layer}
    update_index_settings $index_name
    report_index $index_name
done

log "Done!"
