#!/usr/bin/env python

import argparse
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("topic")
parser.add_argument("filename")
args = parser.parse_args()

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open(args.filename) as messages_file:
    for message in messages_file.readlines():
        producer.send(args.topic, key=b'tracking.ScreenTrackingTriggered', value=message.encode())

producer.flush()
