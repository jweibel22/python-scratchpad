import logging

import boto3

glue = boto3.client('glue')


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_tables(database, token=None):

    if token is not None:
        tables = glue.get_tables(DatabaseName=database, MaxResults=100, NextToken=token)
    else:
        tables = glue.get_tables(DatabaseName=database, MaxResults=100)

    for table in tables['TableList']:
        yield table['Name']

    if 'NextToken' in tables:
        for table in get_tables(database, tables['NextToken']):
            yield table


def delete_tables(database, tables):
    response = glue.batch_delete_table(
        DatabaseName=database,
        TablesToDelete=tables
    )
    if 'Errors' in response and len(response['Errors']) > 0:
        for error in response['Errors']:
            table = error['TableName']
            message = error['ErrorDetail']['ErrorMessage']
            logging.error(f"{table}: {message}")
        raise Exception("deleting tables failed")


def is_created_by_crawler(table):
    return "topic_" in table


sources = ['lw-go-events', "eventstreams", "integrationevents"]
for source in sources:
    tables = [table for table in get_tables(source) if is_created_by_crawler(table)]
    # for chunk in chunks(tables, 100):
    #     delete_tables(source, chunk)
    for table in tables:
        print(table)
