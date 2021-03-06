#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf


def run(conf, connection):
    command = jdbcconf.javacommand(connection)
    command.append("kr.co.penta.datalake.version.Application")
    proc = subprocess.Popen(command, stdin=None,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.communicate()
    print(connection["CNNC_MANAGE_NO"]+" "+connection["SYS_NM"], flush=True)
    if proc.returncode == 0:
        print(out[0].decode("utf-8"), flush=True)
    else:
        print(out[1].decode("utf-8"), flush=True)


def jdbcversion(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    for arg in argv:
        connection = jdbcconf.connection(arg)
        run(conf, connection)


def main():
    if len(sys.argv) < 2:
        print("사용법: jdbcversion {연결관리번호}...", flush=True)
        sys.exit(1)
    jdbcversion(sys.argv[1:])


if __name__ == "__main__":
    main()
