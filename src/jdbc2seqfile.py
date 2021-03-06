#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import threading

import jdbc2json
import jdbcconf
import jdbcutil

name = "JDBC2SEQFILE"

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


def run(jdbc, hive, conf, batch_date, connection, table, partition=None, exit=False):
    global exitcode
    returncode = 0
    stdout = None
    stderr = None
    if partition is not None:
        result = jdbcutil.location(conf, hive, partition, connection, table)
    else:
        result = jdbcutil.location(conf, hive, batch_date, connection, table)
    location = result[0]
    if location is not None:
        url = location + "/" + batch_date
        command = jdbcconf.javacommand(connection)
        if conf["csv"] is not None:
            if partition is not None:
                command.append("-Dcsv={0}{1}-{2}-{3}-{4}.source.csv".format(
                    conf["csv"], batch_date, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"], partition))
            else:
                command.append("-Dcsv={0}{1}-{2}-{3}.source.csv".format(
                    conf["csv"], batch_date, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]))
        command.append("kr.co.penta.datalake.jdbc2seqfile.Application")
        command.append(jdbcutil.jdbcselect(conf, connection, table, False))
        command.append(url)
        proc = subprocess.Popen(
            command, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = proc.communicate()
        returncode = proc.returncode
        stdout = out[0].decode("utf-8")
        stderr = out[1].decode("utf-8")
    else:
        returncode = 1
        stderr = result[1]
    if exit:
        lock.acquire()
        if returncode == 0:
            print("테이블 불러오기 성공: {0}".format(
                jdbcutil.hivetablename(connection, table)), flush=True)
        else:
            print("테이블 불러오기 실패: {0}".format(
                jdbcutil.hivetablename(connection, table)), flush=True)
            if stdout is not None:
                print(stdout, flush=True)
            if stderr is not None:
                print(stderr, flush=True)
            exitcode = 1
        lock.release()
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
        print("사용법: jdbc2seqfile {배치기준시간} {연결관리번호} [테이블명]...", flush=True)
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
