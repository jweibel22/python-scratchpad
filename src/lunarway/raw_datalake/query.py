
import boto3
import json

from lunarway.hour_partition import DayPartition

S3_BUCKET = 'lunarway-prod-data-raw-datalake'

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


def count_lines(object_path):
    print(object_path)
    r = s3.select_object_content(
            Bucket=S3_BUCKET,
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

    raise (Exception("sss"))


def find_object(object_path, id):
    print(object_path)
    r = s3.select_object_content(
            Bucket=S3_BUCKET,
            Key=object_path,
            ExpressionType='SQL',
            Expression=f"select s.Content.id from s3object s where s.Content.id = '{id}'",
            InputSerialization={'JSON': {"Type": "Lines"}},
            OutputSerialization={'JSON': {}}
    )
    event_stream = r['Payload']
    for event in event_stream:
        # If we received a records event, write the data to a file
        if 'Records' in event:
            data = event['Records']['Payload']
            print(data)
            return data




bucket = s3_resource.Bucket(S3_BUCKET)


def count_events(partition):
    # prefix = f"""source={source}/topic={topic}/day={year}-{month}-{day}/hour={hour}"""
    prefix = partition.path()
    files = bucket.objects.filter(Prefix=prefix)
    return sum(count_lines(file.key) for file in files)


# record_count = count_lines("source=lw-go-events/topic=tracking.ScreenTrackingTriggered/day=1970-01-01/hour=0/part-0-120")
# print(record_count)

day_partition = DayPartition("eventstreams", "bankaccount.bankaccount.TransactionPosted.V1", "2020-06-19")
count = count_events(day_partition)
print(count)
