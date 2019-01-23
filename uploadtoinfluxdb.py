#!/usr/bin/env python3
import glob
import argparse
import re
import os
import sys
import datetime
import logging
from influxdb import InfluxDBClient


def datafromlog(src):
    types = ['bw', 'lat', 'clat', 'slat', 'iops']
    date = datetime.datetime.strptime("2019.01.01 00:00:00.000000", "%Y.%m.%d %H:%M:%S.%f")
    for t in types:
        if re.match(r'.*' + t + r'\.\d+\.log', os.path.basename(src)):
            type = t
    with open(src, 'r') as f:
        datalog = []
        for line in f:
            linelist = line.rstrip('\n').split(', ')
            datalog.append(
                {'time': date + datetime.timedelta(milliseconds=int(linelist[0])), 'value': int(linelist[1])})
    return datalog, type

def upload(src=None, args=None):
    datalog, type = datafromlog(src)
    logname = os.path.basename(src)
    fiometric = [{"measurement": "fio",
                  "tags": {"logname": logname,
                           "depth": int(logname.split('_')[2][1:]), "bs": logname.split('_')[1], "type": type,
                           "testtype": logname.split('_')[0], "storagetype": args.storagetype},
                  "time": x["time"].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                  "fields": {"value": x["value"]}} for x in datalog]
    client = InfluxDBClient(host=args.influxdbhost, port=args.influxdbport, username=args.influxdbuser,
                            password=args.influxdbpassword, database=args.influxdb_db)
    # result = client.query('DROP SERIES FROM fio;')
    result = client.write_points(fiometric)
    return result

def dropseries(args):
    client = InfluxDBClient(host=args.influxdbhost, port=args.influxdbport, username=args.influxdbuser,
                            password=args.influxdbpassword, database=args.influxdb_db)
    result = client.query('DROP SERIES FROM fio;')
    return result





def main():
    parser = argparse.ArgumentParser(description='Plot fio logs')
    parser.add_argument('--sourcedir', '-s', dest='sourcedir', default=None, required=False,
                        help='Directory with fio logs')
    parser.add_argument('--pattern', '-p', dest='pattern', default="*.*.log", required=False,
                        help='Pattern for files')
    parser.add_argument('--perl-regexp', '-P', dest='perlpattern', default=None, required=False,
                        help='Interpret  PATTERNS  as  Perl-compatible  regular  expressions  (PCREs).')
    parser.add_argument('--influxdb-user', '-u', dest='influxdbuser', default=None, required=False,
                        help='InfluxDB user')
    parser.add_argument('--influxdb-password', dest='influxdbpassword', default=None, required=False,
                        help='InfluxDB password')
    parser.add_argument('--influxdb-host', dest='influxdbhost', default="localhost", required=False,
                        help='InfluxDB host')
    parser.add_argument('--influxdb-port', dest='influxdbport', default=8086, type=int, required=False,
                        help='InfluxDB port')
    parser.add_argument('--influxdb-db', '-d', dest='influxdb_db', default="fio", required=False,
                        help='InfluxDB Database')
    parser.add_argument('--storage', dest='storagetype', default=None, required=False,
                        help='Measured storage type')
    parser.add_argument('--drop', dest='drop', default=None, required=False, action='store_true',
                        help='Drop series from fio. Dangerous')

    args = parser.parse_args()
    if args.drop:
        print(dropseries(args))
        sys.exit(0)
    if not args.sourcedir:
        print('Specify source directory')
        sys.exit(0)
    elif not args.storagetype:
        print('Specify sourcetype')
        sys.exit(0)
    if args.perlpattern:
        pattern = re.compile(args.perlpattern, flags=0)
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, '*')) if pattern.match(os.path.basename(srcfile))]
    else:
        files = [srcfile for srcfile in glob.glob(os.path.join(args.sourcedir, args.pattern))]
    print(files)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()    # create console handler and set level to debug
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # create formatter
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    for file in files:
        result = upload(src=file, args=args)
        if result:
            logger.info(f'File {file} uploaded to influxdb')
        else:
            logger.error(f'File {file} NOT uploaded to influxdb')





if __name__ == "__main__":
    main()
