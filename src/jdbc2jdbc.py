#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf
import jdbcutil


def run(conf, batch_date, connection1, connection2, table1, table2):
    command = jdbcconf.javacommand2(connection1, connection2)
    command.append("-Ddestination.truncate={0}".format(jdbcutil.jdbcdelete(connection2, table2)))
    command.append("-Ddestination.batch=1000")
    command.append("-Ddestination.commit=2147483647")
    command.append("kr.co.penta.datalake.jdbc2jdbc.Application")
    command.append(jdbcutil.jdbcselect(conf, connection1, table1, False))
    command.append(jdbcutil.jdbcinsert(connection2, table2))
    proc = subprocess.Popen(command, stdin=None)
    proc.communicate()
    sys.exit(proc.returncode)


def jdbc2jdbc(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = argv[0]
    connection1 = jdbcconf.connection(argv[1])
    table1 = jdbcconf.table(connection1, argv[2])
    connection2 = jdbcconf.connection(argv[3])
    table2 = jdbcconf.table(connection2, argv[4])
    run(conf, batch_date, connection1, connection2, table1, table2)


def main():
    if len(sys.argv) < 6:
        print(
            "사용법: jdbc2jdbc {배치기준시간} {원천연결관리번호} {원천테이블명} {대상연결관리번호} {대상테이블명}", flush=True)
        sys.exit(1)
    jdbc2jdbc(sys.argv[1:])


if __name__ == "__main__":
    main()
