
from datetime import datetime
import re


class ParseException(Exception):
    pass


class HourPartition:

    def __init__(self, source, topic, date, hour):
        self.source = source
        self.topic = topic
        self.date = date
        self.hour = hour

    def path(self):
        return f"source={self.source}/topic={self.topic}/day={self.date}/hour={self.hour}"


def parse_hour_partition(path):
    x = r"source=([\w\.\-]+)/topic=([\w\.\-]+)/day=([\w\.\-]+)/hour=(\w+)/([\w\.\-]+)"
    m = re.search(x, path)
    if m is None:
        raise ParseException("invalid hour partition path: " + path)
    date = datetime.strptime(m.group(3), '%Y-%m-%d').date()
    hour = int(m.group(4))

    return HourPartition(source=m.group(1), topic=m.group(2), date=date, hour=hour)


class DayPartition:

    def __init__(self, source, topic, date):
        self.source = source
        self.topic = topic
        self.date = date

    def path(self):
        return f"source={self.source}/topic={self.topic}/day={self.date}"


def parse_day_partition(path):
    x = r"source=([\w\.\-]+)/topic=([\w\.\-]+)/day=([\w\.\-]+)/([\w\.\-]+)"
    m = re.search(x, path)
    if m is None:
        raise ParseException("invalid day partition path: " + path)
    date = datetime.strptime(m.group(3), '%Y-%m-%d').date()

    return DayPartition(source=m.group(1), topic=m.group(2), date=date)
