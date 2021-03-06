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
    returncode = 0
    stdout = None
    stderr = None
    try: 
        hive.execute_update("\n".join(table["QUERY"]["TRUNCATE"]))
        if exit:
            lock.acquire()
            if returncode == 0:
                print("테이블 비우기 성공: {0}".format(
                    jdbcutil.hivetablename(connection, table)), flush=True)
            else:
                print("테이블 비우기 실패: {0}".format(
                    jdbcutil.hivetablename(connection, table)), flush=True)
                print(stderr, flush=True)
                exitcode = 1
            lock.release()
    except Exception as e:
        stderr = str(e)
        returncode = 1
    return [returncode, stdout, stderr]


def worker(tables):
    hive = jdbc2json.new(conf["hive"], connection)
    while len(tables) > 0:
        table = None
        lock.acquire()
        if len(tables) > 0:
            tablename = sorted(tables.keys()).pop(0)
            table = tables[tablename]
            del tables[tablename]
        lock.release()
        if table is not None:
            run(None, hive, conf, None, connection, table, None, True)


def main():
    if len(sys.argv) < 2:
        print("사용법: hivetruncatetable {연결관리번호} [테이블명]...", flush=True)
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
