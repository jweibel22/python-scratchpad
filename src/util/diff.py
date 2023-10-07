#!/usr/bin/env python3

import sys


filename_left = sys.argv[1]
filename_right = sys.argv[2]

with open(filename_left) as file_left:
    content_left = file_left.readlines()

with open(filename_right) as file_right:
    content_right = file_right.readlines()


set_left = set([x.strip() for x in content_left])
set_right = set([x.strip() for x in content_right])

left_missing = set_left - set_right
right_missing = set_right - set_left


print("Left: ", len(set_left))
print("Right: ", len(set_right))
print("Left - Right: ", len(left_missing))
print("Right - Left: ", len(right_missing))

if len(left_missing) > 0:
    print("Left - Right: {}".format(list(left_missing)[:10]))

if len(right_missing) > 0:
    print("Right - Left: {}".format(list(right_missing)[:10]))

