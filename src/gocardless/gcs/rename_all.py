#!/usr/bin/env python

import sys
from google.cloud import storage
import re
from datetime import timedelta, datetime
import subprocess


def floor(dt, delta):
    return dt - (dt - datetime.min) % delta


def new_name(blob_name, created_at):

    datetime_format = "%Y-%m-%dT%H:%M:%S.000Z"
    window_end = floor(created_at.replace(tzinfo=None), timedelta(minutes=30))
    window_start = window_end - timedelta(hours=1)
    m = re.search('([\\w\\-\\.]+)/(.+)(\\-pane\\-.+\\.avro)', blob_name)
    return '{folder}/{window_start}-{window_end}{suffix}'.format(
        folder=m.group(1),
        window_start=window_start.strftime(datetime_format),
        window_end=window_end.strftime(datetime_format),
        suffix=m.group(3))


client = storage.Client()
bucket_name = sys.argv[1]
bucket = client.get_bucket(bucket_name)
for blob in bucket.list_blobs():
    if '-290308-12-21T20:00:00.000Z' in blob.name:
        name = new_name(blob.name, blob.time_created)
        cmd = f'gsutil mv gs://{bucket_name}/{blob.name} gs://{bucket_name}/{name}'
        print(cmd)
        subprocess.run(cmd, shell=True, check=True)

