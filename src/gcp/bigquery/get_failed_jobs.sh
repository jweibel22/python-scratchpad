#!/bin/bash

bq ls -j --max_results=50 | grep " query " | grep " FAILURE " | awk '{print $1}' \
| while read -r job_id; do bq show --format=prettyjson -j "$job_id"; done \
| jq 'select(.status.errors != null)' \
| jq '.configuration.query.query, .status.errorResult.message'
