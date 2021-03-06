#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys

import jdbcconf


def run(conf, argv):
    command = jdbcconf.javacommand({})
    command.append("kr.co.penta.datalake.common.Aes")
    command.extend(argv)
    proc = subprocess.Popen(command, stdin=None)
    out = proc.communicate()
    sys.exit(proc.returncode)


def main():
    jdbcconf.home()
    conf = jdbcconf.conf()
    run(conf, sys.argv[1:])


if __name__ == "__main__":
    main()
