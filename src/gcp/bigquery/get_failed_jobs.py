#!/usr/bin/env python

from google.cloud import bigquery

client = bigquery.Client()
jobs = client.list_jobs(max_results=50, state_filter='DONE')
failed_jobs = (job for job in jobs if job.job_type == 'query' and job.errors is not None)

for job in failed_jobs:
    print("Query: " + job.query)
    print("Error: " + job.errors[0]['message'])
