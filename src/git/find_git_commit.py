
import subprocess
import os


def find(root_dir, look_for):
    for subdir in os.listdir(root_dir):
        if '.' not in subdir:
            git_dir = os.path.join(root_dir, subdir)
            if os.path.isdir(os.path.join(git_dir, '.git')):
                result = subprocess.Popen('git reflog | grep {look_for}'.format(look_for=look_for),
                                       cwd=git_dir,
                                       shell=True,
                                       stdout=subprocess.PIPE).stdout.read()
                if result:
                    print subdir
                    print result


find('/Users/jimmy.rasmussen/git/just-data', "timezone")
