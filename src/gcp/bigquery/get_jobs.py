import subprocess
import json

cmd = "bq ls -j --max_results=300 just-data | tail -n +4 | awk '{print $1, $2, $3}'"
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)

ids = []

for line in iter(proc.stdout.readline, ''):
    x = line.rstrip().split(' ')
    if x[1] == 'query' and x[2] == 'FAILURE':
        ids.append(x[0])


def get_job_details(id):
    cmd = "bq show --format=prettyjson -j {id}".format(id=id) # | jq '.configuration.query.query, .status.errorResult.message'
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    res = json.loads(proc.stdout.read())
    return res


jobs = (get_job_details(id) for id in ids)
jimmys_jobs = (job for job in jobs if job['user_email'] == 'jimmy.rasmussen@just-eat.com' and job['status']['errorResult']['reason'] != 'notFound')

for job_details in jimmys_jobs:
    print job_details['configuration']['query']['query']
    print job_details['status']['errorResult']['message']
    print '\n'
