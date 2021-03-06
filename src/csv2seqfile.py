#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys

import jdbc2json
import jdbcconf
import jdbcutil


def run(hive, conf, batch_date, connection, filename, table, partition=None, exit=False):
    if partition is not None:
        result = jdbcutil.location(conf, hive, partition, connection, table)
    else:
        result = jdbcutil.location(conf, hive, batch_date, connection, table)
    location = result[0]
    if location is not None:
        url = location + "/" + batch_date
        command = jdbcconf.javacommand(None)
        command.append("kr.co.penta.datalake.csv2seqfile.Application")
        command.append(filename)
        command.append(url)
        proc = subprocess.Popen(command, stdin=None)
        proc.communicate()
        if exit:
            sys.exit(proc.returncode)
        else:
            return proc.returncode
    elif exit:
            print(result[1], flush=True)
            sys.exit(1)
    return 1


def csv2seqfile(argv):
    cwd = os.getcwd()
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = argv[0]
    connection = jdbcconf.connection(argv[1])
    filename = argv[2]
    if not filename.startswith(os.path.sep):
        filename = cwd + os.path.sep + filename
    table = jdbcconf.table(connection, argv[3])
    hive = jdbc2json.new(conf["hive"], connection)
    run(hive, conf, batch_date, connection, filename, table, None, True)


def main():
    if len(sys.argv) < 5:
        print("사용법: csv2seqfile {배치기준시간} {연결관리번호} {파일명} {테이블명}", flush=True)
        sys.exit(1)
    csv2seqfile(sys.argv[1:])


if __name__ == "__main__":
    main()
