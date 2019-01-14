#!/usr/bin/env python3
import glob
import argparse
import sys
import os
import asyncio
import concurrent.futures


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
            dstfile.write(f'{self.seconds_in_ms * second}, {sum(valsum)}, {row[2]}, {row[3]}\n')
        return f'{src} processed'



    async def asynclogprepare(self, executor, args):
        loop = asyncio.get_event_loop()
        blocking_tasks = [
            loop.run_in_executor(executor, self.logprepare, log["src"], log["dst"])
            for log in args
        ]
        completed, pending = await asyncio.wait(blocking_tasks)
        results = [t.result() for t in completed]
        print('results: {!r}'.format(results))


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
    event_loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor()

    task_list = []
    for file in glob.glob(os.path.join(args.sourcedir, args.pattern)):
        print(f'Processing {file} to {os.path.join(args.destdir, os.path.basename(file))}')
        task_list.append({"src": file, "dst": os.path.join(args.destdir, os.path.basename(file))})
    try:
         event_loop.run_until_complete(logp.asynclogprepare(executor, task_list))
    finally:
        event_loop.close()
