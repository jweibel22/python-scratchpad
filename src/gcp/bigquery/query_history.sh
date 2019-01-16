#!/bin/bash

bq ls -j --max_results=50 | grep " query " | awk '{print $1}' \
| while read -r job_id; do bq show --format=prettyjson -j "$job_id"; done \
| jq '"\(.configuration.query.destinationTable.datasetId).\(.configuration.query.destinationTable.tableId)"'
