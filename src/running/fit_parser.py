import os
import glob
import sys

from fitparse import FitFile
import pandas as pd


def get_field(record, name):
    if record.get(name) is not None:
        return record.get(name).value
    else:
        return None


def record_to_row(index, record):
    return [
        index+1,
        get_field(record, 'start_time'),
        get_field(record, 'total_timer_time'),
        get_field(record, 'total_elapsed_time'),
        get_field(record, 'total_distance'),
        get_field(record, 'total_ascent'),
        get_field(record, 'total_descent'),
        get_field(record, 'max_heart_rate'),
        get_field(record, 'avg_heart_rate'),
        get_field(record, 'max_speed'),
        get_field(record, 'total_strides'),
    ]



garmin_root_path = sys.argv[1] #'/Users/jimmyrasmussen/Dropbox/garmin'
data_path = os.path.join(garmin_root_path, 'data')

imports_filename = os.path.join(garmin_root_path, 'imports.txt')
activities_filename = os.path.join(garmin_root_path, 'activities.csv')

all_laps = []
idx = 1
filenames = []

with open(imports_filename, 'r') as f:
    filenames = f.read().splitlines()

with open(activities_filename, 'r') as f:
    all_laps = [x.split(',') for x in f.read().splitlines()[1:]]

for filepath in glob.glob(f"{data_path}/*.fit"):
    print(f"Processing {idx}")
    idx += 1
    filename = os.path.basename(filepath)
    if filename not in filenames:
        print("Importing file " + filename)
        fitfile = FitFile(os.path.join(data_path, filename))
        fitfile.parse()
        records = list(fitfile.get_messages(name='lap'))
        for i in range(len(records)):
            lap = record_to_row(i, records[i])
            all_laps.append(lap)

        filenames.append(filename)

df = pd.DataFrame(all_laps, columns=[
    'lap_number',
    'start_time',
    'total_timer_time',
    'total_elapsed_time',
    'total_distance',
    'total_ascent',
    'total_descent',
    'max_heart_rate',
    'avg_heart_rate',
    'max_speed_mm_sec',
    'total_strides'])
df.to_csv(activities_filename, index=False)

with open(imports_filename, 'w+') as f:
    f.writelines(["%s\n" % item for item in filenames])
