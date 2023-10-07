import boto3

glue = boto3.client('glue')


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


def get_table_versions(database, table, token=None):

    if token is not None:
        table_versions = glue.get_table_versions(DatabaseName=database, TableName=table, MaxResults=100, NextToken=token)
    else:
        table_versions = glue.get_table_versions(DatabaseName=database, TableName=table, MaxResults=100)

    for table_version in table_versions['TableVersions']:
        yield table_version['VersionId']

    if 'NextToken' in table_versions:
        for table_version in get_table_versions(database, table, table_versions['NextToken']):
            yield table_version


def delete_deprecated_versions(database, table):
    version_ids = list(get_table_versions(database, table))[1:]
    if len(version_ids) > 0:
        print(table)
        print(f"Found: {version_ids}")
        sliced = [version_ids[i:i + 100] for i in range(0, len(version_ids), 100)]
        for slice in sliced:
            glue.batch_delete_table_version(DatabaseName=database, TableName=table, VersionIds=slice)


database = 'lw-go-events'

for table in get_tables(database):
    print(f"cleaning table {table}")
    delete_deprecated_versions(database, table)
