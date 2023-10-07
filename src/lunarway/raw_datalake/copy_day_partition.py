import datetime
import json

import boto3
import re
from datetime import timedelta, date
import subprocess

# from lunarway.hour_partition import parse_hour_partition
from lunarway.hour_partition import parse_day_partition


s3 = boto3.resource('s3')

class ParseException(Exception):
    pass


class HourPartition:

    def __init__(self, source, topic, date, hour):
        self.source = source
        self.topic = topic
        self.date = date
        self.hour = hour


def parse_hour_partition(path):
    x = r"source=([\w\.\-]+)/topic=([\w\.\-]+)/day=([\w\.\-]+)/hour=(\w+)/([\w\.\-]+)"
    m = re.search(x, path)
    if m is None:
        raise ParseException("invalid hour partition path: " + path)
    date = datetime.datetime.strptime(m.group(3), '%Y-%m-%d').date()
    hour = int(m.group(4))

    return HourPartition(source=m.group(1), topic=m.group(2), date=date, hour=hour)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def count_records(s3_bucket, object_path):
    s3_client = boto3.client('s3')
    r = s3_client.select_object_content(
            Bucket=s3_bucket,
            Key=object_path,
            ExpressionType='SQL',
            Expression="select count(*) from s3object s",
            InputSerialization={'JSON': {"Type": "Lines"}},
            OutputSerialization={'JSON': {}}
    )

    for event in r['Payload']:
        if 'Records' in event:
            payload = json.loads(event['Records']['Payload'].decode('utf-8'))
            return int(payload["_1"])

    raise Exception("Unexpected response from S3")


class RawDatalake(object):

    def __init__(self, bucket, dry):
        self.bucket = bucket
        self.dry = dry

    def sources(self):
        x = r"source=([\w\.\-]+)/"
        result = self.bucket.meta.client.list_objects(Bucket=self.bucket.name,
                                                 Delimiter='/')
        for o in result.get('CommonPrefixes'):
            m = re.search(x, o.get('Prefix'))
            if m is not None:
                yield m.group(1)

    def topics(self, source):
        x = r"source=([\w\.\-]+)/topic=([\w\.\-]+)"
        result = self.bucket.meta.client.list_objects(Bucket=self.bucket.name,
                                                 Prefix=f"source={source}/",
                                                 Delimiter='/')

        if result.get('CommonPrefixes') is None:
            return

        for o in result.get('CommonPrefixes'):
            m = re.search(x, o.get('Prefix'))
            topic = m.group(2)
            yield topic

    def day_partitions(self, source, topic):
        x = r"source=([\w\.\-]+)/topic=([\w\.\-]+)/day=([\w\.\-]+)"
        result = self.bucket.meta.client.list_objects(Bucket=self.bucket.name,
                                                      Prefix=f"source={source}/topic={topic}/",
                                                      Delimiter='/')

        if result.get('CommonPrefixes') is None:
            return

        for o in result.get('CommonPrefixes'):
            m = re.search(x, o.get('Prefix'))
            topic = m.group(3)
            yield topic

    def copy_day(self, day, destination_bucket_name):
        for source in self.sources():
            for topic in self.topics(source):
                self._copy_topic_day(source, topic, day, destination_bucket_name)

    def _copy_topic_day(self, source, topic, day, destination_bucket_name):
        prefix = f"source={source}/topic={topic}/day={day}"

        for obj in list(self.bucket.objects.filter(Prefix=prefix)):
            print(f"copying {obj.key}")
            if not self.dry:
                s3.Object(destination_bucket_name, obj.key).copy_from(CopySource=f"{self.bucket.name}/{obj.key}")

    def copy_topic_to_redundancy_bucket(self, source, topic, start_date, end_date, destination_bucket_name):
        prefix = f"source={source}/topic={topic}"

        for obj in list(self.bucket.objects.filter(Prefix=prefix)):
            day_partition = parse_day_partition(obj.key)

            if end_date > day_partition.date >= start_date:
                filename = obj.key.split('/')[-1]
                destination_key = f"source={day_partition.source}/day={day_partition.date}/{topic.replace('.', '_')}-{filename}"
                print(f"copying {obj.key} to {destination_key}")
                if not self.dry:
                    s3.Object(destination_bucket_name, destination_key).copy_from(
                        CopySource=f"{self.bucket.name}/{obj.key}")

    def copy_hour(self, day, hour, destination_bucket_name):
        for source in self.sources():
            for topic in self.topics(source):
                self._copy_topic_hour(source, topic, day, hour, destination_bucket_name)

    def _copy_topic_hour(self, source, topic, day, hour, destination_bucket_name):
        prefix = f"source={source}/topic={topic}/day={day}/hour={hour}"

        for obj in list(self.bucket.objects.filter(Prefix=prefix)):
            print(f"copying {obj.key}")
            if not self.dry:
                s3.Object(destination_bucket_name, obj.key).copy_from(CopySource=f"{self.bucket.name}/{obj.key}")

    def delete_day(self, day):
        for source in self.sources():
            for topic in self.topics(source):
                prefix = f"source={source}/topic={topic}/day={day}/"
                for obj in list(self.bucket.objects.filter(Prefix=prefix)):
                    print(f"deleting {obj.key}")
                    if not self.dry:
                        s3.Object(self.bucket.name, obj.key).delete()

    def delete_hour(self, day, hour):
        for source in self.sources():
            for topic in self.topics(source):
                prefix = f"source={source}/topic={topic}/day={day}/hour={hour}"
                for obj in list(self.bucket.objects.filter(Prefix=prefix)):
                    if obj.last_modified.hour == 20:
                        print(f"deleting {obj.key} - {obj.last_modified.hour}")
                        if not self.dry:
                            s3.Object(self.bucket.name, obj.key).delete()

                    # try:
                    #     number = int(obj.key.split("-")[-1])
                    #     if number > 1000:
                    #         # print(obj.key.split("-")[-1])
                    #         print(f"deleting {obj.key} - {obj.last_modified.hour}")
                    #         # s3.Object(self.bucket.name, obj.key).delete()
                    # except Exception:
                    #     pass


compact_raw_datalake_bucket = 'lunarway-prod-data-compact-raw-datalake'


# def all_topics():
#     source = "lw-go-events"
#     s3 = boto3.resource('s3')
#     raw_datalake = RawDatalake(s3.Bucket(compact_raw_datalake_bucket), dry=True)
#     for topic in raw_datalake.topics(source):
#         yield topic



def xx(raw_datalake):
    source = "lw-go-events"
    for topic in raw_datalake.topics(source):
        for p in raw_datalake.day_partitions(source, topic):
            yield (source, topic, p)



# s3 = boto3.resource('s3')
# raw_datalake = RawDatalake(s3.Bucket('lunarway-prod-data-compact-raw-datalake'), dry=True)
#
# partitions = xx(raw_datalake)
# for (s,t,p) in partitions:
#     if p == '1970-01-01':
#         print(t)

# start_date = date(2015, 12, 17)
# end_date = date(2020, 9, 3)
# raw_datalake = RawDatalake(s3.Bucket(compact_raw_datalake_bucket), dry=False)
#
# for topic in topics:
#     raw_datalake.copy_topic_to_redundancy_bucket("lw-go-events", topic, start_date, end_date,
#                                                   'lunarway-prod-data-redundant-raw-datalake')
