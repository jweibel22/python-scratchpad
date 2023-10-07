
import datetime
import re

import boto3
import logging

from lunarway.hour_partition import parse_hour_partition, ParseException, parse_day_partition


env = "prod"

s3 = boto3.resource('s3')
client = boto3.client("s3")
raw_bucket = s3.Bucket(f'lunarway-{env}-data-raw-datalake')
compact_raw_bucket = s3.Bucket(f'lunarway-{env}-data-compact-raw-datalake')
jwr_bucket = s3.Bucket('lunarway-dev-data-jwr') if env == "dev" else s3.Bucket('lunarway-data-jwr')

structured_bucket = s3.Bucket(f'lunarway-{env}-data-structured-datalake')


class EventStreamsTopic:

    default_namespaces = {
        'accountnumberseries': 'bankaccount',
        'bankaccount': 'bankaccount',
        'bankaccountcatalog': 'bankaccount',
        'bankaccounting': 'bankaccount',
        'becaccount': 'becadapter',
        'becaccounting': 'becadapter',
        'becnemkonto': 'becadapter',
        'user': 'cardmanagement',
    }

    def __init__(self, namespace, event_stream_name, event_name, version):
        self.namespace = namespace
        self.event_stream_name = event_stream_name
        self.event_name = event_name
        self.version = version

    @staticmethod
    def parse_new(s):
        x = r"([\w]+)\.([\w]+)\.([\w]+)\.V([\w]+)"
        m = re.search(x, s)
        if m is None:
            raise ParseException("invalid topic: " + s)
        return EventStreamsTopic(m.group(1), m.group(2), m.group(3), m.group(4))

    @staticmethod
    def parse_old(s):
        x = r"^([a-zA-Z0-9]+)\.([a-zA-Z0-9]+)$"
        m = re.search(x, s)
        if m is None:
            raise ParseException("invalid topic: " + s)

        event_stream_name = m.group(1)
        event_name = m.group(2)

        return EventStreamsTopic(EventStreamsTopic.default_namespaces[event_stream_name], event_stream_name, event_name, 1)

    def to_string(self):
        return f"{self.namespace}.{self.event_stream_name}.{self.event_name}.V{self.version}"


def move(bucket_name, path, new_path):
    copy_source = {
        'Bucket': bucket_name,
        'Key': path
    }
    s3.meta.client.copy(copy_source, bucket_name, new_path)
    s3.Object(bucket_name, path).delete()


def rename_all(bucket, hour_partitioned, dry_run=False):

    for obj in bucket.objects.filter(Prefix='source=eventstreams/'):
        try:
            partition = parse_hour_partition(obj.key) if hour_partitioned else parse_day_partition(obj.key)
            filename = obj.key.split("/")[-1]
            partition.topic = EventStreamsTopic.parse_old(partition.topic).to_string()
            new_path = f"{partition.path()}/{filename}"
            print(f"Renaming {obj.key} to {new_path}")
            if not dry_run:
                move(bucket.name, obj.key, new_path)
        except ParseException:
            pass


# rename_all(structured_bucket, False, dry_run=False)
# rename_all(jwr_bucket, True, dry_run=False)
# rename_all(raw_bucket, True, dry_run=False)
# rename_all(compact_raw_bucket, False, dry_run=False)
