

import sys
import re
from optparse import OptionParser

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("--column", dest="column",
        help="column")
parser.add_option("--pattern", dest="pattern",
        help="patten")

column_idx = 1
search_string = 'query'
options, args = parser.parse_args()


for line in sys.stdin.readlines():
    x = list(filter(lambda y: y != '', re.split('\s+', line)))
    if x[int(options.column)] == options.pattern:
        sys.stdout.write("%s\n" % line.strip())


# bq ls -j --max_results=1 | ./filter_csv.py --column=1 --pattern=query | awk '{print $1}' | while read -r line; do bq show --format=prettyjson -j "$line"; done
#How to do the map on a stream in an efficient way?
    #using the cli? xargs, a while loop that turns it back into a stream? or using python that takes the cli command to call as a lambda?