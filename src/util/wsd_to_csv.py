#!/usr/bin/env python
import sys
import re

for line in sys.stdin.readlines():
    x = filter(lambda y: y != '', re.split('\s+', line))
    sys.stdout.write("%s\n" % ','.join(x))
