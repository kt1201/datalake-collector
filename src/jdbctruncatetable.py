#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "HIVETRUNCATE"

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
    global exitcode
    name = jdbcutil.jdbctablename(connection, table)
    error = None
    try:
        jdbc.execute_update("TRUNCATE TABLE {0}".format(name))
    except:
        try:
            jdbc.execute_update("DELETE FROM {0}".format(name))
        except:
            error = jdbc.error()
    if error is None:
        lock.acquire()
        print("테이블 비우기 성공: {0}".format(name), flush=True)
        lock.release()
    else:
        lock.acquire()
        print("테이블 비우기 실패: {0}".format(name), flush=True)
        print(error, flush=True)
        exitcode = 1
        lock.release()


def worker(tables):
    jdbc = jdbc2json.new(connection)
    while len(tables) > 0:
        table = None
        lock.acquire()
        if len(tables) > 0:
            tablename = sorted(tables.keys()).pop(0)
            table = tables[tablename]
            del tables[tablename]
        lock.release()
        if table is not None:
            run(jdbc, None, conf, None, connection, table, None, True)


def main():
    if len(sys.argv) < 2:
        print("사용법: jdbctruncatetable {연결관리번호} [테이블명]...", flush=True)
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
