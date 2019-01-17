#!/usr/bin/env python3
import glob
import argparse
import re
import os
import multiprocessing
import datetime
import pandas as pd
import matplotlib.pyplot as plt

class LogPlot(object):
    def __init__(self):
        self.seconds_in_ms = 1000


    def plot(self, src):
        types = {'bw': 'Bandwith MiB/sec', 'lat': 'Latency in msecs', 'clat': ' Completion  latency in msecs',
                'slat': 'Submission latency in msecs', 'iops': 'IO per second'}

        for t in types.keys():
            if re.match(r'.*'+t+r'\.\d+\.log', os.path.basename(src)):
                type = {"name": t, "title": types[t]}
        with open(src, 'r') as f:
            out = []
            for line in f:
                linelist = line.rstrip('\n').split(', ')
                out.append({'Time': datetime.timedelta(milliseconds=int(linelist[0])), type.get("name"): int(linelist[1])})
        df = pd.DataFrame(out)
        df.Time = pd.to_datetime(df.Time).dt.time
        df.set_index('Time', inplace=True)
        if type.get('name') == 'bw':
            df = df.apply(lambda x: x/1024, axis=1)
        if 'lat' in type.get('name'):
            df = df.apply(lambda x: x/1000000, axis=1)
        pplot = df.plot()
        pplot.set(xlabel="Time", ylabel=type.get("title"), title=os.path.basename(src))
        plt.show()



def main():
    parser = argparse.ArgumentParser(description='Plot fio logs')
    parser.add_argument('--sourcedir', '-s', dest='sourcedir', default=None, required=True,
                        help='Directory with fio logs')
    parser.add_argument('--pattern', '-p', dest='pattern', default="*.*.log", required=False,
                        help='Pattern for files')
    parser.add_argument('--perl-regexp', '-P', dest='perlpattern', default=None, required=False,
                        help='Interpret  PATTERNS  as  Perl-compatible  regular  expressions  (PCREs).')

    args = parser.parse_args()
    logp = LogPlot()
    if args.perlpattern:
        pattern = re.compile(args.perlpattern, flags=0)
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, '*')) if pattern.match(os.path.basename(srcfile))]
    else:
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))]
    print(files)
    with multiprocessing.Pool(processes=len(files)) as pool:
        results = pool.map(logp.plot, files)



if __name__ == "__main__":
    main()
