import datetime

import boto3

from lunarway.hour_partition import DayPartition

S3_BUCKET = 'lunarway-prod-data-raw-datalake'

s3 = boto3.client('s3', region_name='eu-west-1')


def last_modified_time(day_partition):
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=day_partition.path()
    )
    if response['IsTruncated']:
        raise Exception("response was truncated, not supported")

    timestamps = [file['LastModified'] for file in response['Contents']]
    return max(timestamps)


base = datetime.datetime.strptime("2020-06-09", "%Y-%m-%d")
num_days = 11
date_list = [base + datetime.timedelta(days=x) for x in range(num_days)]

day_partitions = [DayPartition("eventstreams", "bankaccount.bankaccount.TransactionPosted.V1", date.strftime("%Y-%m-%d")) for date in date_list]

for day_partition in day_partitions:
    ts = last_modified_time(day_partition)
    print(f"{day_partition.date} - {ts}")
