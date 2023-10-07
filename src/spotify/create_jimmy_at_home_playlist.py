from spotify.spotify import get_all, create_playlist, add_tracks_to_playlist
from spotify.utils import batch

jimmy_id = '113179990'

playlist_name = 'jimmys_at_home'
tracks = [track for track in get_all() if track['audio_features']['energy'] < 0.4]
playlist_id = create_playlist(jimmy_id, playlist_name)

for track_batch in batch(tracks, 100):
    add_tracks_to_playlist(playlist_id, [track['id'] for track in track_batch])
