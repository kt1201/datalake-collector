#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "HIVEANALYZE"

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


def hiveddl(partition, connection, table, name):
    kwds = {"DBTYPE": connection["DB_TY_NM"], "BATCH_DATE": partition,
            "APD_CHK_COL": table["APD_CHK_COL"]}
    partition_spec = []
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is None:
            partition_spec.append(
                column["HIVE_COL_NM"] + " = " + jdbcutil.jdbcexpression(column, kwds))
    query = "ANALYZE TABLE {0}".format(name)
    if len(partition_spec) > 0:
        query = query + " PARTITION ( {0} )".format(", ".join(partition_spec))
    query = query + " COMPUTE STATISTICS"
    return query


def run(jdbc, hive, conf, batch_date, connection, table, partition=None, exit=False):
    global exitcode
    returncode = 0
    stdout = None
    stderr = None
    if partition is None:
        partition = batch_date
    name = jdbcutil.hivetablename(connection, table)
    query = hiveddl(partition, connection, table, name)
    try:
        hive.execute_update(query)
        if exit:
            lock.acquire()
            if returncode == 0:
                print("통계 생성 성공: {0}".format(name), flush=True)
            else:
                print("통계 생성 실패: {0}".format(name), flush=True)
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
            run(None, hive, conf, batch_date, connection, table, None, True)


def main():
    if len(sys.argv) < 3:
        print("사용법: hiveanalyze {배치기준시간} {연결관리번호} [테이블명]...", flush=True)
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
