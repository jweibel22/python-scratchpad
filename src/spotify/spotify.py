import requests
import json


LIMIT = 50

headers = {
    'Authorization': 'Bearer xxx'
}


def print_header():
    print("id,artist,name,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,duration_ms,time_signature")


def print_track(track):
    audio_features = track['audio_features']
    fields = [
        track['id'],
        track['artist'],
        track['title'],
        audio_features['danceability'],
        audio_features['energy'],
        audio_features['key'],
        audio_features['loudness'],
        audio_features['mode'],
        audio_features['speechiness'],
        audio_features['acousticness'],
        audio_features['instrumentalness'],
        audio_features['liveness'],
        audio_features['valence'],
        audio_features['tempo'],
        audio_features['duration_ms'],
        audio_features['time_signature'],
        ]

    strings = [str(x) for x in fields]
    print(','.join(strings))


def get_audio_features(ids):
    url = f'https://api.spotify.com/v1/audio-features?ids={ids}'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.text)

    output = json.loads(response.text)
    return output['audio_features']


def get_all():
    return get_from_offset(0)


def get_from_offset(offset):
    url = f'https://api.spotify.com/v1/me/tracks?offset={offset}&limit={LIMIT}'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.text)

    output = json.loads(response.text)
    tracks = [item['track'] for item in output['items']]
    ids = [track['id'] for track in tracks]
    all_audio_features = get_audio_features(','.join(ids))

    for track in tracks:
        id = track['id']
        name = track['name']
        artist = track['artists'][0]['name']
        audio_features = [x for x in all_audio_features if x['id'] == id]

        xx = {
            'id': id,
            'title': name,
            'artist': artist,
            'audio_features': audio_features[0],
        }
        yield xx

    total = output['total']
    if offset + LIMIT < total:
        for x in get_from_offset(offset + LIMIT):
            yield x


def create_playlist(user_id, name):
    payload = {
        'name': name,
        'public': False,
    }
    response = requests.post(f'https://api.spotify.com/v1/users/{user_id}/playlists', json=payload, headers=headers)
    if response.status_code != 201:
        raise Exception(response.text)

    return json.loads(response.text)['id']


def add_tracks_to_playlist(playlist_id, track_ids):
    uris = [f'spotify:track:{id}' for id in track_ids]
    payload = {
        'uris': uris,
    }
    response = requests.post(f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks', json=payload, headers=headers)
    if response.status_code != 201:
        raise Exception(response.text)
