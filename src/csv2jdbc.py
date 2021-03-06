#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys

import jdbcconf
import jdbcutil


def run(conf, connection, filename, table):
    command = jdbcconf.javacommand(connection)
    command.append("kr.co.penta.datalake.csv2jdbc.Application")
    command.append(filename)
    command.append(jdbcutil.jdbcinsert(connection, table))
    proc = subprocess.Popen(command, stdin=None)
    proc.communicate()
    sys.exit(proc.returncode)


def csv2jdbc(argv):
    cwd = os.getcwd()
    jdbcconf.home()
    conf = jdbcconf.conf()
    connection = jdbcconf.connection(argv[0])
    filename = argv[1]
    if not filename.startswith(os.path.sep):
        filename = cwd + os.path.sep + filename
    table = jdbcconf.table(connection, argv[2])
    run(conf, connection, filename, table)


def main():
    if len(sys.argv) < 4:
        print("사용법: csv2jdbc {연결관리번호} {파일명} {테이블명}", flush=True)
        sys.exit(1)
    csv2jdbc(sys.argv[1:])


if __name__ == "__main__":
    main()
