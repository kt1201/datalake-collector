#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys

import jdbcconf
import jdbcutil


def run(conf, batch_date, connection, table, filename):
    command = jdbcconf.javacommand(connection)
    command.append("kr.co.penta.datalake.jdbc2csv.Application")
    command.append(jdbcutil.jdbcselect(conf, connection, table, False))
    command.append(filename)
    proc = subprocess.Popen(command, stdin=None)
    proc.communicate()
    sys.exit(proc.returncode)


def jdbc2csv(argv):
    cwd = os.getcwd()
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = argv[0]
    connection = jdbcconf.connection(argv[1])
    table = jdbcconf.table(connection, argv[2])
    filename = argv[3]
    if not filename.startswith(os.path.sep):
        filename = cwd + os.path.sep + filename
    run(conf, batch_date, connection, table, filename)


def main():
    if len(sys.argv) < 5:
        print("사용법: jdbc2csv {배치기준시간} {연결관리번호} {테이블명} {파일명}", flush=True)
        sys.exit(1)
    jdbc2csv(sys.argv[1:])


if __name__ == "__main__":
    main()
