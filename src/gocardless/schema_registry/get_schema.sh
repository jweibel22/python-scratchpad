#!/usr/bin/env bash

if [ $1 = "staging" ]
then
    curl https://avro:$SCHEMA_REGISTRY_STAGING_PASSWORD@avro-staging.gocardless.io/subjects/$2/versions/1 | jq '.schema' | jq -r
elif [ $1 = "prod" ]
then
    curl https://avro:$SCHEMA_REGISTRY_PRODUCTION_PASSWORD@avro.gocardless.io/subjects/$2/versions/1 | jq '.schema' | jq -r
else
    echo "unknown environment $1"
fi
