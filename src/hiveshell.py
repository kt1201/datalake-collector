#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf


def run(conf, connection):
    command = []
    command.append("beeline")
    if conf["hive"]["classpath"] is not None:
        command.append("-addlocaldriverjar")
        command.append(conf["hive"]["classpath"])
    if conf["hive"]["classname"] is not None:
        command.append("-d")
        command.append(conf["hive"]["classname"])
    if conf["hive"]["url"] is not None:
        command.append("-u")
        command.append(conf["hive"]["url"].format(**connection))
    if conf["hive"]["username"] is not None:
        command.append("-n")
        command.append(conf["hive"]["username"])
    if conf["hive"]["password"] is not None:
        command.append("-p")
        command.append(conf["hive"]["password"])
    proc = subprocess.Popen(command)
    proc.communicate()
    sys.exit(proc.returncode)


def hiveshell(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    connection = {"HIVE_DB_NM": ""}
    if len(argv) >= 1:
        connection = jdbcconf.connection(argv[0])
    run(conf, connection)


def main():
    if len(sys.argv) < 1:
        print("사용법: hiveshell [연결관리번호]", flush=True)
        sys.exit(1)
    hiveshell(sys.argv[1:])


if __name__ == "__main__":
    main()
