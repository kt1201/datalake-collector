#!/usr/bin/python3
# -*- coding]\nutf-8 -*-

import copy
import json
import sys
import threading

import jdbcconf
import jdbcutil
import hivecreatetable
import hivedroppartition
import hivecreatepartition
import jdbc2json
import jdbc2seqfile
import hiveanalyze
import hiveverify
import hiveclearpartition

name = "DailyTable"
clearpartition = False
handlers = [hivecreatetable, hivedroppartition, hivecreatepartition,
            jdbc2seqfile, hiveanalyze, hiveverify, hiveclearpartition]

min_partitions = 7
max_partitions = 14

conf = {}
batch_date = None
connection = {}
tables = {}
partitions = {}
pool = []
lock = threading.Lock()
exitcode = 0


def get_tables(jdbc, batch_date, connection, table):
    if jdbc is None:
        jdbc = jdbc2json.new(connection)
    records = jdbc.tables(
        schema=connection["DB_ACNT_NM"], table=table["TABLE_ENG_NM"])
    tables = []
    for record in records:
        if record["TABLE_NAME"][-8:] < batch_date[0:8]:
            tables.append(record["TABLE_NAME"])
    tables.sort(reverse=True)
    return tables


def load(jdbc, hive, conf, batch_date, connection, table, partition):
    status = {}
    for handler in handlers:
        result = handler.run(jdbc, hive, conf, batch_date,
                             connection, table, partition)
        if result[0] != 0:
            result[2] = "[{0}]\n{1}".format(handler.name, result[2])
            return result
        status = jdbcutil.merge(status, result[1])
    return [0, json.dumps(status, ensure_ascii=False), None]


def run(jdbc, hive, conf, batch_date, connection, table):
    exitcode = 0
    limit = max_partitions
    results = {"Records": 0, "MD5": {}}
    tables = get_tables(None, batch_date, connection, table)
    partitions = jdbcutil.get_partitions(hive, connection, table)
    if len(tables) < limit:
        limit = len(tables)
    for index in range(limit):
        yyyymmdd = tables[index][-8:]
        partition = yyyymmdd + "000000"
        if index >= min_partitions and partition in partitions:
            continue
        tableyyyymmdd = copy.deepcopy(table)
        tableyyyymmdd["TABLE_ENG_NM"] = tableyyyymmdd["TABLE_ENG_NM"][:-1] + yyyymmdd
        result = load(jdbc, hive, conf, batch_date,
                      connection, tableyyyymmdd, partition)
        if result[0] != 0:
            return result
        status = json.loads(result[1])
        results["Records"] += status["Records"]
        results["MD5"].update(status["MD5"])
    for index in range(limit, len(tables)):
        yyyymmdd = tables[index][-8:]
        partition = yyyymmdd + "000000"
        if partition not in partitions:
            print("파티션 수동적재 필요: {0}: {1}={2}000000".format(
                jdbcutil.hivetablename(connection, table), table["PART_COLS"], partition))
            exitcode = 1
    if exitcode != 0:
        return [exitcode, json.dumps(results, ensure_ascii=False), "파티션 수동적재 필요"]
    return [0, json.dumps(results, ensure_ascii=False), None]


def initalize():
    global conf, batch_date, connection, tables, partitions
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = sys.argv[1]
    connection = jdbcconf.connection(sys.argv[2])
    table = jdbcconf.table(connection, sys.argv[3])
    jdbc = jdbc2json.new(connection)
    hive = jdbc2json.new(conf["hive"], connection)
    records = get_tables(jdbc, batch_date, connection, table)
    partitions = jdbcutil.get_partitions(hive, connection, table)
    args = None
    if len(sys.argv) > 4:
        args = {}
        for arg in sys.argv[4:]:
            name = table["TABLE_ENG_NM"][:-1] + arg
            args[name] = None
    tables = {}
    for record in records:
        if args is not None:
            if record in args:
                tables[record] = copy.deepcopy(table)
                tables[record]["TABLE_ENG_NM"] = record
                del args[record]
        else:
            tables[record] = copy.deepcopy(table)
            tables[record]["TABLE_ENG_NM"] = record
    if args is not None:
        if len(args) > 0:
            print("테이블을 찾을 수 없음: {0}".format(
                list(args.keys())), file=sys.stderr, flush=True)
            sys.exit(1)


def worker(tables):
    jdbc = jdbc2json.new(connection)
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
            partition = table["TABLE_ENG_NM"][-8:] + "000000"
            lock.acquire()
            if partition in partitions:
                print("파티션 있음 건너뛰기: {0}".format(partition), flush=True)
            else:
                result = load(jdbc, hive, conf, batch_date,
                              connection, table, partition)
                if result[0] == 0:
                    status = json.loads(result[1])
                    verify = True
                    for key in status["MD5"].keys():
                        if status["MD5"][key][0] != status["MD5"][key][1]:
                            verify = False
                    if verify == True:
                        print("파티션 불러오기 성공: {0}".format(partition), flush=True)
                    else:
                        print("파티션 해시 불일치: {0}".format(partition), flush=True)
                else:
                    print("파티션 불러오기 실패: {0}".format(partition), flush=True)
            lock.release()


def main():
    if len(sys.argv) < 4:
        print(
            "사용법: python3 src/load004.py {배치기준시간} {시스템명} {테이블명} [YYYYMMDD]...", flush=True)
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
