#!/usr/bin/env python3

import sys
from datetime import datetime

for line in sys.stdin.readlines():
    try:
        ts = int(line)
    except ValueError:
        ts = 0

    sys.stdout.write("%s\n" % datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"))
