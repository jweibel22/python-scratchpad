#!/bin/bash

bq ls --max_results=500 gc_paysvc_live_production | grep "TABLE" | awk '{print $1}'\
| while read -r table_name; do bq show --format=prettyjson "gc_paysvc_live_production.$table_name"; done \
| jq -r '"\(.numBytes),\(.id)"'