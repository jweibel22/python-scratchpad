#!/usr/bin/env bash

echo "deploying version $1"
# docker build -t ingestion-gateway:local .
# docker tag ingestion-gateway:local eu.gcr.io/poc-gc-data-platform/ingestion-gateway:$1
# docker push eu.gcr.io/poc-gc-data-platform/ingestion-gateway:$1
kubectl -n dev set image deployment/ingestion-gateway console=eu.gcr.io/poc-gc-data-platform/ingestion-gateway:$1
