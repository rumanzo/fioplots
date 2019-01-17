#!/usr/bin/env python3
import glob
import argparse
import re
import os
import multiprocessing
import datetime
import pandas as pd
from functools import partial
from pathlib import Path
import matplotlib.pyplot as plt

class LogPlot(object):

    def plot(self, src=None, dst=None, show=None, pformat=None):
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
        pplot = df.plot(figsize=(18, 9))
        pplot.set(xlabel="Time", ylabel=type.get("title"), title=os.path.basename(src))
        if dst:
            plt.savefig(os.path.join(dst, f'{Path(src).stem}.{pformat}'))
        if show:
            plt.show()





def main():
    parser = argparse.ArgumentParser(description='Plot fio logs')
    parser.add_argument('--sourcedir', '-s', dest='sourcedir', default=None, required=True,
                        help='Directory with fio logs')
    parser.add_argument('--pattern', '-p', dest='pattern', default="*.*.log", required=False,
                        help='Pattern for files')
    parser.add_argument('--perl-regexp', '-P', dest='perlpattern', default=None, required=False,
                        help='Interpret  PATTERNS  as  Perl-compatible  regular  expressions  (PCREs).')
    parser.add_argument('--savepath', '-S', dest='savepath', default=None, required=False,
                        help='Save dir path for plots')
    parser.add_argument('--format', '-f', dest='pformat', default='png', required=False,
                        help='Save path for plots')
    parser.add_argument('--show', dest='show', default=None, required=False, action='store_true',
                        help='Show plots')

    args = parser.parse_args()
    logp = LogPlot()
    if args.perlpattern:
        pattern = re.compile(args.perlpattern, flags=0)
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, '*')) if pattern.match(os.path.basename(srcfile))]
    else:
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))]
    print(files)
    with multiprocessing.Pool() as pool:
        f1 = partial(logp.plot, dst=args.savepath)
        f2 = partial(f1, show=args.show)
        f3 = partial(f2, pformat=args.pformat)
        results = pool.map(f3, files)



if __name__ == "__main__":
    main()
