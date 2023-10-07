import json
from datetime import datetime, timedelta
import requests

client_id = '45814'
client_secret = '5b0867ad4dbe23b31a7a1193332d1f194d7631d8'
# auth_code = '85be6e853737246ad2b06883541b4880dc2a3442'
refresh_token = '4b3b83d8ba112fe6f4ed5aefd8b03ac929952644'


def start_of_week():
    dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = dt - timedelta(days=dt.weekday())
    return round(datetime.timestamp(start))


def refresh_access_token():
    url = f'https://www.strava.com/api/v3/oauth/token'
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        raise Exception(response.text)
    output = json.loads(response.text)
    return output


def get_activities(access_token, after):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    url = f'https://www.strava.com/api/v3/athlete/activities?after={after}'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.text)

    output = json.loads(response.text)
    return output


access_token = refresh_access_token()['access_token']

after = start_of_week()
activities = get_activities(access_token, after)
distances = [a['distance'] for a in activities]
print(round(sum(distances))/1000)


