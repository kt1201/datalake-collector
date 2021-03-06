#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import threading

import jdbcconf
import jdbcutil


name = "HIVE2JDBC"

conf = {}
connection = {}
tables = {}
pool = []
lock = threading.Lock()
exitcode = 0


def initalize():
    global conf, connection, tables
    jdbcconf.home()
    conf = jdbcconf.conf()
    connection = jdbcconf.connection(sys.argv[1])
    tables = jdbcconf.tables(conf, connection, sys.argv[2:])


def run(jdbc, hive, conf, batch_date, connection, table, partition=None, exit=False):
    returncode = 0
    stdout = None
    stderr = None
    command = jdbcconf.javacommand2(conf["hive"], connection)
    command.append("-Ddestination.truncate={0}".format(jdbcutil.jdbcdelete(connection, table)))
    command.append("-Ddestination.batch=1000")
    command.append("-Ddestination.commit=2147483647")
    command.append("kr.co.penta.datalake.jdbc2jdbc.Application")
    command.append(jdbcutil.jdbcselect2(conf, connection, table, False))
    command.append(jdbcutil.jdbcinsert(connection, table))
    proc = subprocess.Popen(command, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.communicate()
    returncode = proc.returncode
    stdout = out[0].decode("utf-8")
    stderr = out[1].decode("utf-8")
    if exit:
        lock.acquire()
        if returncode == 0:
            print("테이블 복제 성공: {0}".format(
                jdbcutil.hivetablename(connection, table)), flush=True)
        else:
            print("테이블 복제 실패: {0}".format(
                jdbcutil.hivetablename(connection, table)), flush=True)
            if stdout is not None:
                print(stdout, flush=True)
            if stderr is not None:
                print(stderr, flush=True)
            exitcode = 1
        lock.release()
    return [returncode, stdout, stderr]


def worker(tables):
    while len(tables) > 0:
        table = None
        lock.acquire()
        if len(tables) > 0:
            tablename = sorted(tables.keys()).pop(0)
            table = tables[tablename]
            del tables[tablename]
        lock.release()
        if table is not None:
            run(None, None, conf, None, connection, table, None, True)

def main():
    if len(sys.argv) < 2:
        print(
            "사용법: hive2jdbc {연결관리번호} [테이블명]...", flush=True)
        sys.exit(1)
    initalize()
    if conf["threads"] > 1:
        for index in range(conf["threads"]):
            pool.append(
                threading.Thread(target=worker, name="Worker {0}".format(index), args=(tables,)))
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join()
    else:
        worker(tables)
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
