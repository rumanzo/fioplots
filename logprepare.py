#!/usr/bin/env python3
import glob
import argparse
import sys
import os

class Logprepare(object):
    def __init__(self):
        self.seconds_in_ms = 1000

    def logprepare(self, src, dst):
        with open(src, 'r') as srcfile, open(dst, 'w') as dstfile:
            rows = list(srcfile)
            rowslen = len(rows)
            second = 1
            valsum = []
            for num, line in enumerate(rows):
                row = line.rstrip('\n').split(', ')  # [ms, value, type, bs]
                if num == rowslen:
                    valsum.append(int(row[1]))
                    dstfile.write(f'{self.seconds_in_ms * second}, {sum(valsum)}, {row[2]}, {row[3]}\n')
                elif int(row[0]) < self.seconds_in_ms * second + 1:
                    valsum.append(int(row[1]))
                else:
                    dstfile.write(f'{self.seconds_in_ms * second}, {sum(valsum)}, {row[2]}, {row[3]}\n')
                    second += 1
                    valsum.clear()
                    valsum.append(int(row[1]))
        return True

    def getparsedlog(self, src):
        newrows = []
        with open(src, 'r') as srcfile:
            rows = list(srcfile)
            rowslen = len(rows)
            second = 1
            valsum = []
            for num, line in enumerate(rows):
                row = line.rstrip('\n').split(', ')  # [ms, value, type, bs]
                if num == rowslen:
                    valsum.append(int(row[1]))
                    newrows.append([self.seconds_in_ms * second, sum(valsum), row[2], row[3]])
                if int(row[0]) < self.seconds_in_ms * second + 1:
                    valsum.append(int(row[1]))
                else:
                    newrows.append([self.seconds_in_ms * second, sum(valsum), row[2], row[3]])
                    second += 1
                    valsum.clear()
                    valsum.append(int(row[1]))
        return newrows







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
    print(glob.glob(os.path.join(args.sourcedir, args.pattern)))
    for file in glob.glob(os.path.join(args.sourcedir, args.pattern)):
        print(file, os.path.join(args.destdir, os.path.basename(file)))
        logp.logprepare(file, os.path.join(args.destdir, os.path.basename(file)))