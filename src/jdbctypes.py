#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf


def run(conf, connection, query):
    command = jdbcconf.javacommand(connection)
    command.append("kr.co.penta.datalake.types.Application")
    command.append(query)
    proc = subprocess.Popen(command, stdin=None)
    proc.communicate()
    sys.exit(proc.returncode)


def jdbcquery(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    connection = jdbcconf.connection(argv[0])
    query = argv[1]
    run(conf, connection, query)


def main():
    if len(sys.argv) < 3:
        print("사용법: jdbctypes {연결관리번호} {쿼리}", flush=True)
        sys.exit(1)
    jdbcquery(sys.argv[1:])


if __name__ == "__main__":
    main()
