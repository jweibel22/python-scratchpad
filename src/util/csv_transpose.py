#!/usr/bin/env python

import csv
import sys

rows = list(csv.reader(sys.stdin))
writer = csv.writer(sys.stdout)
for col in xrange(0, len(rows[0])):
    writer.writerow([row[col].strip() for row in rows])
