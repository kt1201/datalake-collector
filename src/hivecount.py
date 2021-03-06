#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "HIVECOUNT"

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


def hiveselect(table, name):
    kwds = {"DBTYPE": connection["DB_TY_NM"], "BATCH_DATE": batch_date,
            "APD_CHK_COL": table["APD_CHK_COL"]}
    query = "SELECT COUNT(*) AS COUNT FROM {0}".format(name)
    where_clause = []
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is None:
            where_clause.append(
                column["HIVE_COL_NM"] + " = " + jdbcutil.jdbcexpression(column, kwds))
    if len(where_clause) > 0:
        query = query + " WHERE " + " AND ".join(where_clause)
    return query


def run(hive, table):
    global exitcode
    name = jdbcutil.hivetablename(connection, table)
    try:
        records = hive.execute_query(hiveselect(table, name))
        lock.acquire()
        if "COUNT" in records[0]:
            print("{0}\t{1}".format(name, records[0]["COUNT"]), flush=True)
        else:
            print("{0}\t{1}".format(name, "COUNT 값이 없음"), flush=True)
            exitcode = 1
        lock.release()
    except Exception as e:
        lock.acquire()
        print("{0}\t{1}".format(name, e), flush=True)
        exitcode = 1
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
