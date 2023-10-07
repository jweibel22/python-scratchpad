import pandas as pd


def load_activities():
    activities = pd.read_csv('/Users/jimmyrasmussen/Dropbox/garmin/activities.csv', parse_dates=True).loc['2015-01-01':'2023-01-01'] #.sort_values('start_time') index_col=1
    # activities['Year'] = activities.index.year
    # activities['Week'] = activities.index.week
    # activities['start_day'] = pd.to_datetime(activities.index).date
    activities['start_day'] = pd.to_datetime(activities['start_time']).dt.date
    return activities


def get_sum_by_week(activities):
    sum_by_week = activities[activities.Year > 2019].groupby(by=['Year', 'Week'], as_index=True)['Year', 'Week', 'total_distance', 'total_timer_time'].sum()
    sum_by_week['avg_pace'] = pd.to_timedelta((sum_by_week['total_timer_time']/60)/(sum_by_week['total_distance']/1000), unit='minutes')
    sum_by_week['time'] = pd.to_timedelta(sum_by_week['total_timer_time'], unit='seconds')
    sum_by_week['distance'] = sum_by_week['total_distance']/1000
    return sum_by_week


def load_knee_condition():
    df = pd.read_csv('knee.csv', parse_dates=True)
    # df['start_day'] = pd.to_datetime(df.index).date
    return df
