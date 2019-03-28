#!/usr/bin/env python
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-q', action='store_true')
args = parser.parse_args()

lines = [f"{l.strip()}" for l in sys.stdin.readlines()]
if args.q:
    # lines = [f"'{l}'" for l in lines]
    lines = [f'"{l}"' for l in lines]

sys.stdout.write("%s" % ','.join(lines))
