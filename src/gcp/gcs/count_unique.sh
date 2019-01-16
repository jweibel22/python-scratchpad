#!/usr/bin/env bash

 find . -name '*.avro' -exec avro-tools tojson {} \; > all.jsonl
 find . -name '*.avro' -exec avro-tools tojson {} \; | jq '.payload.currency' | sort -u | wc -l


 comm -3 <(sort pg_authorisation_links.tsv) <(sort bq_authorisation_links.csv)

