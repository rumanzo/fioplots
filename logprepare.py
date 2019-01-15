#!/usr/bin/env python3
import glob
import argparse
import sys
import os
import multiprocessing
from functools import partial


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

    def mplogprepare(self, destdir, src):
        dstfile = os.path.join(destdir, os.path.basename(src))
        result = self.logprepare(src, dstfile)
        return result


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

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        func = partial(logp.mplogprepare, args.destdir)
        results = pool.map(func, [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))])
    print(results)
