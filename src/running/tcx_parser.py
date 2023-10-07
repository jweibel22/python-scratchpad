import os
import sys
import glob
import re
from datetime import datetime
import xmlschema
from pprint import pprint
import pandas as pd

path = '/Users/jimmyrasmussen/Downloads/garminexport'


class TcxFile(object):

    def __init__(self, filename):
        self.filename = filename
        self.datetime = self.parse_filename(filename)
        self.size = os.path.getsize(filename)

    @staticmethod
    def parse_filename(filename):
        m = re.match(f"{path}/(.*)_[\d]+.tcx", filename)
        return datetime.fromisoformat(m.group(1))


xs = xmlschema.XMLSchema('schema.xsd')


tcx_files = [TcxFile(f) for f in glob.glob(f"{path}/*.tcx")]
# filtered = [file for file in tcx_files if file.datetime < datetime(2019, 7, 9, 0, 0, 0, 0, pytz.UTC) and file.datetime > datetime(2011, 12, 18, 0, 0, 0, 0, pytz.UTC)]
# filtered.sort(key=sort_key)

exercise = xs.to_dict(tcx_files[0].filename)
activity = exercise['Activities']['Activity'][0]
lap = activity['Lap'][0]
pprint(lap)


def save_to_csv(files):
    all_laps = []
    idx = 1
    for file in files:

        print(f"Processing {idx}")
        idx += 1

        exercise = xs.to_dict(file.filename)
        activity = exercise['Activities']['Activity'][0]
        for lap in activity['Lap']:
            if 'AverageHeartRateBpm' in lap:
                heart_rate = lap['AverageHeartRateBpm']['Value']
            else:
                heart_rate = 0.0

            all_laps.append([lap['@StartTime'], lap['DistanceMeters'],lap['TotalTimeSeconds'], heart_rate])
        # laps = [[lap['@StartTime'], lap['DistanceMeters'],lap['TotalTimeSeconds'], lap['AverageHeartRateBpm']['Value']] for lap in activity['Lap']]

    df = pd.DataFrame(all_laps, columns=['start_time', 'distance_meters', 'total_time_seconds', 'average_heart_rate_bpm'])
    df.to_csv('activities.csv', index=False)
    # print(df.to_string())

