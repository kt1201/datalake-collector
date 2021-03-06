#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf


def run(conf, connection):
    command = []
    command.append("beeline")
    if connection["JDBC_CLASSPATH"] is not None:
        if len(connection["JDBC_CLASSPATH"]) > 0:
            command.append("-addlocaldriverjar")
            command.append(connection["JDBC_CLASSPATH"])
    if connection["JDBC_DRIVER"] is not None:
        if len(connection["JDBC_DRIVER"]) > 0:
            command.append("-d")
            command.append(connection["JDBC_DRIVER"])
    if connection["JDBC_URL"] is not None:
        if len(connection["JDBC_URL"]) > 0:
            command.append("-u")
            command.append(connection["JDBC_URL"])
    if connection["DB_USER_ID"] is not None:
        if len(connection["DB_USER_ID"]) > 0:
            command.append("-n")
            command.append(connection["DB_USER_ID"])
    if connection["DB_USER_SECRET_NO"] is not None:
        if len(connection["DB_USER_SECRET_NO"]) > 0:
            command.append("-p")
            command.append(connection["DB_USER_SECRET_NO"])
    proc = subprocess.Popen(command)
    proc.communicate()
    sys.exit(proc.returncode)


def jdbcshell(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    connection = jdbcconf.connection(argv[0])
    run(conf, connection)


def main():
    if len(sys.argv) < 2:
        print("사용법: jdbcshell {연결관리번호}", flush=True)
        sys.exit(1)
    jdbcshell(sys.argv[1:])


if __name__ == "__main__":
    main()
