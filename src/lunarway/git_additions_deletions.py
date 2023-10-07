import os

import requests
import json
import logging
from datetime import datetime, timezone
import sys

github_token = os.environ.get('GITHUB_TOKEN')

headers = {
    'Authorization': f'Bearer {github_token}'
}


root = logging.getLogger()
root.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
root.addHandler(handler)


class Changes:
    def __init__(self, date, additions, deletions):
        self.deletions = deletions
        self.additions = additions
        self.date = date


org = "lunarway"


def list_repos(org):
    # there is approx. 518 repos in total so we hard code the total_pages
    total_pages = 7
    for page in range(1, total_pages):
        url = f"https://api.github.com/orgs/{org}/repos?sort=full_name&per_page=100&page={page}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"request failed: {response.status_code}. {response.text}")

        repos = [entry['name'] for entry in json.loads(response.text)]
        for repo in repos:
            yield repo


def weekly_activity(org, name):
    url = f"https://api.github.com/repos/{org}/{name}/stats/code_frequency"
    response = requests.get(url, headers=headers)

    if response.status_code == 204:
        logging.warning(f"no data for {name}")
        return

    if response.status_code != 200 and response.status_code != 202:
        raise Exception(f"request failed: {response.status_code}. {response.text}")

    for entry in json.loads(response.text):
        utc_time = datetime.fromtimestamp(entry[0], timezone.utc)
        yield Changes(utc_time, entry[1], entry[2])


if __name__ == "__main__":

    repos = list(list_repos(org))
    for repo in repos:
        logging.info(f"processing {repo}")
        for entry in weekly_activity(org, repo):
            print(f"{repo},{entry.date.strftime('%Y-%m-%d')},{entry.additions},{entry.deletions}")
