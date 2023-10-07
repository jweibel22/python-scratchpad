import boto3

from athena import AWSAthenaHook
from lunarway.raw_datalake.copy_day_partition import RawDatalake

s3 = boto3.resource('s3')

raw_datalake = RawDatalake(s3.Bucket('lunarway-prod-data-compact-raw-datalake'), dry=True)

session = boto3.Session()
client = session.client('athena', region_name='eu-west-1')
athena = AWSAthenaHook(client)


def run_query(query_str):
    query_execution_context = {'Database': 'raw_data_lake'}
    result_configuration = {'OutputLocation': f"s3://lunarway-prod-data-logs/athena-results/"}
    query_id = athena.run_query(query_str, query_execution_context, result_configuration)
    query_status = athena.poll_query_status(query_id, None)
    if query_status in AWSAthenaHook.FAILURE_STATES:
        raise Exception("query failed")


for source in raw_datalake.sources():
    for topic in raw_datalake.topics(source):
        print(f"{source}/{topic}")
        # for day_partition in raw_datalake.day_partitions(source, topic):
        #     query_str = f"ALTER TABLE all ADD IF NOT EXISTS PARTITION (day='{day_partition}', topic='{topic}',source='{source}')"
        #     print(query_str)
            # run_query(query_str)
