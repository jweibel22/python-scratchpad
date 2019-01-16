#!/usr/bin/env python
import sys
import re

for line in sys.stdin.readlines():
    parts = filter(lambda y: y != '', re.split('\s+', line))
    quoted_parts = map(lambda part: "\"{part}\"".format(part=part), parts)
    sys.stdout.write("[%s]\n" % ','.join(quoted_parts))
        
