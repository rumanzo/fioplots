#!/usr/bin/env python3
import glob
import argparse
import sys
import os
import multiprocessing
import datetime
import pandas as pd
import matplotlib.pyplot as plt

class LogPlot(object):
    def __init__(self):
        self.seconds_in_ms = 1000


    def plot(self, src):
        with open(src, 'r') as f:
            out = []
            for line in f:
                linelist = line.rstrip('\n').split(', ')
                out.append({'Time': datetime.timedelta(milliseconds=int(linelist[0])), 'bw': int(linelist[1])})
        df = pd.DataFrame(out)
        df.Time = pd.to_datetime(df.Time).dt.time
        # df.set_index('Time')
        df.set_index('Time', inplace=True)
        df = df.apply(lambda x: x/1024, axis=1)
        pplot = df.plot()
        pplot.set(xlabel="Time", ylabel="Bandwith MiB/sec", title=os.path.basename(src))
        plt.show()



def main():
    parser = argparse.ArgumentParser(description='Plot fio logs')
    parser.add_argument('--sourcedir', '-s', dest='sourcedir', default=None, required=True,
                        help='Directory with fio logs')
    parser.add_argument('--pattern', '-p', dest='pattern', default="*.*.log", required=False,
                        help='Pattern for files')
    args = parser.parse_args()

    logp = LogPlot()
    files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))]
    with multiprocessing.Pool(processes=len(files)) as pool:
        # func = partial(logp.mplogprepare, args.destdir)
        # results = pool.map(func, [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))])
        results = pool.map(logp.plot, files)



if __name__ == "__main__":
    main()