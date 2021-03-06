#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "JDBCCOUNT"

conf = {}
batch_date = None
connection = {}
tables = {}
pool = []
lock = threading.Lock()
exitcode = 0


def initalize():
    global conf, batch_date, connection, tables
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = sys.argv[1]
    connection = jdbcconf.connection(sys.argv[2])
    tables = jdbcconf.tables(conf, connection, sys.argv[3:])


def run(jdbc, table):
    global exitcode
    name = jdbcutil.jdbctablename(connection, table)
    try:
        records = jdbc.execute_query(
            "SELECT COUNT(*) AS COUNT FROM {0}".format(name))
        lock.acquire()
        if len(records) == 1:
            if "COUNT" in records[0]:
                print(name + "\t" + str(records[0]["COUNT"]), flush=True)
            else:
                print(records, flush=True)
                exitcode = 1
        else:
            print(records, flush=True)
            exitcode = 1
        lock.release()
    except Exception as e:
        lock.acquire()
        print("{0}".format(e), flush=True)
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
            run(jdbc, table)


def main():
    if len(sys.argv) < 3:
        print("사용법: jdbccount {배치기준시간} {연결관리번호} [테이블명]...", flush=True)
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
