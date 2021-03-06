#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "HIVELISTPARTITION"

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


def hiveddl(table, name):
    partition = False
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is None:
            partition = True
    if not partition:
        return None
    return "SHOW PARTITIONS {0}".format(name)


def run(hive, table):
    global exitcode
    name = jdbcutil.hivetablename(connection, table)
    query = hiveddl(table, name)
    if query is not None:
        try:
            result = hive.execute_query(query)
            lock.acquire()
            if hive.error() is None:
                print("파티션 목록: {0}".format(
                    jdbcutil.hivetablename(connection, table)), flush=True)
                for record in result:
                    print(record["PARTITION"], flush=True)
            lock.release()
        except Exception as e:
            lock.acquire()
            print("파티션 조회 실패: {0}".format(
                jdbcutil.hivetablename(connection, table)), flush=True)
            print("{0}".format(e), flush=True)
            exitcode = 1
            lock.release()
    else:
        lock.acquire()
        print("파티션 없음: {0}".format(
            jdbcutil.hivetablename(connection, table)), flush=True)
        lock.release()


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
            run(hive, table)


def main():
    if len(sys.argv) < 2:
        print("사용법: hivelistpartition {연결관리번호} [테이블명]...", flush=True)
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
