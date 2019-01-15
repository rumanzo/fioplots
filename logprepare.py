#!/usr/bin/env python3
import glob
import argparse
import sys
import os
import threading
from time import sleep


class Logprepare(object):
    def __init__(self):
        self.seconds_in_ms = 1000

    def logprepare(self, src, dst):
        with open(src, 'r') as srcfile, open(dst, 'w') as dstfile:
            second = 1
            valsum = []
            row = []
            for line in srcfile:
                row = line.rstrip('\n').split(', ')  # [ms, value, type, bs]
                if int(row[0]) < self.seconds_in_ms * second + 1:
                    valsum.append(int(row[1]))
                else:
                    dstfile.write(f'{self.seconds_in_ms * second}, {sum(valsum)}, {row[2]}, {row[3]}\n')
                    second += 1
                    valsum.clear()
                    valsum.append(int(row[1]))
            if second != 1:
                dstfile.write(f'{self.seconds_in_ms * second}, {sum(valsum)}, {row[2]}, {row[3]}\n')
        return f'{src} processed'




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare fio logs (truncate) for plots')
    parser.add_argument('--sourcedir', '-s', dest='sourcedir', default=None, required=True,
                        help='Directory with fio logs')
    parser.add_argument('--destdir', '-d', dest='destdir', default=None, required=True,
                        help='Directory for output files')
    parser.add_argument('--pattern', '-p', dest='pattern', default="*.*.log", required=False,
                        help='Pattern for files')
    args = parser.parse_args()
    if args.sourcedir == args.destdir:
        print("Source direcroty must be different from destination directory. Exiting")
        sys.exit(1)

    logp = Logprepare()

    for file in glob.glob(os.path.join(args.sourcedir, args.pattern)):
        print(f'Processing {file} to {os.path.join(args.destdir, os.path.basename(file))}')
        th = threading.Thread(target=logp.logprepare, args=(file, os.path.join(args.destdir, os.path.basename(file))))
        th.start()
    while threading.active_count() != 1:
        sleep(1)
