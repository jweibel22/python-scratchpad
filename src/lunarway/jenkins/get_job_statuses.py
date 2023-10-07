import logging
import os
import sys

import requests
import json
import unittest

from requests.auth import HTTPBasicAuth
import pandas as pd
import itertools


root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
root.addHandler(handler)


class JobsApi:
    def __init__(self, base_url, auth, org):
        self.org = org
        self.base_url = base_url
        self.basic_auth = auth
        self.headers = { }

    def list_jobs(self):
        url = f"{self.base_url}/job/{self.org}/api/json"
        response = requests.get(url, headers=self.headers, auth=self.basic_auth)
        if response.status_code != 200:
            raise Exception(f"request failed: {response.status_code}. {response.text}")

        return (entry['name'] for entry in json.loads(response.text)['jobs'])

    def _build_number(self, node):
        if node is None:
            return -1
        return int(node['number'])

    def job_status(self, job):
        url = f"{self.base_url}/job/{self.org}/job/{job}/job/master/api/json"
        response = requests.get(url, headers=self.headers, auth=self.basic_auth)

        if response.status_code == 404:
            return "undetermined"

        if response.status_code != 200:
            raise Exception(f"request failed: {response.status_code}. {response.text}")

        info = json.loads(response.text)
        last_build = self._build_number(info['lastBuild'])
        last_failed_build = self._build_number(info['lastFailedBuild'])
        last_successful_build = self._build_number(info['lastSuccessfulBuild'])

        if last_build == -1 or (last_successful_build == -1 and last_failed_build == -1):
            return "undetermined"

        if last_build == last_successful_build:
            return "success"

        if last_build == last_failed_build:
            return "failure"

        return "undetermined"

    def all_statuses(self):
        for job in self.list_jobs():
            logging.info(f"fetching {self.org}/{job}")
            status = self.job_status(job)
            yield {
                'job': job,
                'status': status
            }

    def trigger(self, job):
        url = f"{self.base_url}/job/{self.org}/job/{job}/job/master/api/json"
        response = requests.post(url, headers=self.headers, auth=self.basic_auth)
        if response.status_code != 200:
            raise Exception(f"request failed: {response.status_code}. {response.text}")


class Analyser:

    def __init__(self, dev_jobs, platform_jobs):
        self.dev_jobs = list(dev_jobs)
        self.platform_jobs = list(platform_jobs)

    def _group_by_status_and_transpose(self, jenkins, jobs):
        result = pd.DataFrame(jobs, columns=['job', 'status']).groupby(by='status').count().T
        result['jenkins'] = jenkins

        if 'success' not in result:
            result['success'] = 0
        if 'failure' not in result:
            result['failure'] = 0
        if 'undetermined' not in result:
            result['undetermined'] = 0

        return result

    def stats(self):
        count_by_status_dev = self._group_by_status_and_transpose('dev', self.dev_jobs)
        count_by_status_platform = self._group_by_status_and_transpose('platform', self.platform_jobs)
        return pd.concat([count_by_status_dev, count_by_status_platform])[['jenkins', 'success', 'failure', 'undetermined']]

    def failed(self):
        failed_in_dev = [job['job'] for job in filter(lambda job: job['status'] == 'failure', self.dev_jobs)]

        for job in self.platform_jobs:
            if job['status'] == 'failure' and job['job'] not in failed_in_dev:
                yield job['job']


class AnalyserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dev_jobs = [{'job': 'lunar-way-user-service', 'status': 'success'}]
        platform_jobs = [{'job': 'lunar-way-user-service', 'status': 'success'}, {'job': 'lunar-way-authentication-service', 'status': 'failure'}]
        cls.analyser = Analyser(dev_jobs, platform_jobs)

    def testStats(self):
        stats = self.analyser.stats()
        self.assertEqual(2, len(stats), "2 rows in table")
        self.assertEqual('dev', stats.iloc[0,0], 'first row is dev jenkins')
        self.assertEqual(1, stats.iloc[0, 1], '1 success in dev')
        self.assertEqual(0, stats.iloc[0, 2], '0 failures in dev')
        self.assertEqual(0, stats.iloc[0, 3], '0 undetermined in dev')
        self.assertEqual(1, stats.iloc[1, 2], '1 failure in platform')

    def testFailed(self):
        failed = list(self.analyser.failed())
        self.assertEqual(1, len(failed), 'one failed in platform that didnt fail in dev')


if __name__ == "__main__":

    # Supply these env vars:
    # JENKINS_USER: email of a jenkins user
    # PLATFORM_API_TOKEN: An API token that has been generated for the JENKINS_USER
    # DEV_API_TOKEN: An API token that has been generated for the JENKINS_USER

    # fail script if Analyser doesn't pass unit tests
    runner = unittest.TextTestRunner()
    result = runner.run(unittest.makeSuite(AnalyserTest))
    if len(result.errors) > 0 or len(result.failures) > 0:
        exit(1)

    platform = JobsApi(
        base_url="https://jenkins.lunar.tech",
        auth=HTTPBasicAuth(os.environ.get('JENKINS_USER'), os.environ.get('PLATFORM_API_TOKEN')),
        org="Github%20LunarWay%20"
    )
    dev = JobsApi(
        base_url="https://jenkins.dev.lunarway.com",
        auth=HTTPBasicAuth(os.environ.get('JENKINS_USER'), os.environ.get('DEV_API_TOKEN')),
        org="github-lunarway"
    )

    analyser = Analyser(dev.all_statuses(), platform.all_statuses())

    print("\n\nStatistics:")
    stats = analyser.stats()
    print(stats.to_string(index=False))

    print("\n\nFailed in platform but not in dev:\n")
    failed = analyser.failed()
    for job in failed:
        print(job)
