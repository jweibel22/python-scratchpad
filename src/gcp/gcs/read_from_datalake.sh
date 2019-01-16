#!/usr/bin/env bash

gsutil ls $1 |
while read -r line; do gsutil cat "$line" | avro-tools tojson -; done
