import subprocess
import json
import pprint
import re

# this script runs through all sbt dependencies and constructs a graphviz graph
# store this output to a file and run: dot -Tpng myfile.dot -o dep.png  (do a brew install graphviz)

cmd = 'find . -type f \( -name "build.sbt" -o -name "Dependencies.scala" \) -exec grep -e ""com.justeat"" {} \; -print'
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/Users/jimmyrasmussen/git/je')

map = {}
l = []

for line in iter(proc.stdout.readline, ''):
    if "build.sbt" in line or "Dependencies.scala" in line:
        if len(l) > 0:
            map[line.strip()] = l
            l = []
    elif "%" in line:
        l.append(line.strip())

print("strict graph {")
for key, value in map.iteritems():
    m = re.match(r"^\./([\-\w]+)[\"/build.sbt\"|\"/project/Dependencies.scala\"]", key)
    parent = m.group(1)
    for dep in value:
        ff = r'"com.justeat" [%]+ "([\.\_\-\w]+)" %'
        m2 = re.search(ff, dep)
        child = m2.group(1)
        print('"{parent}" -- "{child}"'.format(parent=parent, child=child))
print("}")
