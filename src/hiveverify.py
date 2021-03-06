#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import platform
import subprocess
import sys
import threading

import hive2csv
import jdbcconf
import jdbcutil

name = "HIVEVERIFY"

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


def csvfilename(conf, batch_date, connection, table, partition, suffix):
    if partition is not None:
        filename = "{0}{1}-{2}-{3}-{4}.{5}.csv".format(
            conf["csv"], batch_date, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"], partition, suffix)
    else:
        filename = "{0}{1}-{2}-{3}.{4}.csv".format(
            conf["csv"], batch_date, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"], suffix)
    return filename


def md5command(conf, filename):
    command = []
    if platform.system() == "Darwin":
        command.append("md5")
    elif platform.system() == "Windows":
        command.append("certutil.exe")
        command.append("-hashfile")
    else:
        command.append("md5sum")
    command.append(filename)
    if platform.system() == "Windows":
        command.append("MD5")
    return command


def run(jdbc, hive, conf, batch_date, connection, table, partition=None, exit=False):
    global exitcode
    returncode = 0
    stdout = None
    stderr = None
    sourcemd5 = None
    targetmd5 = None
    sourcefilename = csvfilename(
        conf, batch_date, connection, table, partition, "source")
    targetfilename = csvfilename(
        conf, batch_date, connection, table, partition, "target")
    if partition is not None:
        exitcode = hive2csv.run(
            conf, partition, connection, table, targetfilename)
    else:
        exitcode = hive2csv.run(
            conf, batch_date, connection, table, targetfilename)
    if exitcode != 0:
        stderr = "테이블 추출 실패: {0}".format(
            jdbcutil.hivetablename(connection, table))
        if exit:
            lock.acquire()
            print(stderr, flush=True)
            lock.release()
        returncode = exitcode
    if exitcode == 0:
        command = md5command(conf, sourcefilename)
        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = proc.communicate()
        if proc.returncode == 0:
            if platform.system() == "Darwin":
                sourcemd5 = out[0].decode("utf-8").split("=")[1].strip()
            elif platform.system() == "Windows":
                sourcemd5 = out[0].decode("latin-1").split("\n")[1].strip()
            else:
                sourcemd5 = out[0].decode("utf-8").split(" ")[0]
        else:
            if platform.system() == "Windows":
                sourcemd5 = "00000000000000000000000000000000"
            else:
                returncode = proc.returncode
                exitcode = returncode
                stderr = "소스 해시 생성 실패: {0}: {1}".format(
                    jdbcutil.hivetablename(connection, table), out[1].decode("utf-8"))
                if exit:
                    lock.acquire()
                    print(stderr, flush=True)
                    lock.release()
    if exitcode == 0:
        command = md5command(conf, targetfilename)
        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = proc.communicate()
        if proc.returncode == 0:
            if platform.system() == "Darwin":
                targetmd5 = out[0].decode("utf-8").split("=")[1].strip()
            elif platform.system() == "Windows":
                targetmd5 = out[0].decode("latin-1").split("\n")[1].strip()
            else:
                targetmd5 = out[0].decode("utf-8").split(" ")[0]
        else:
            if platform.system() == "Windows":
                targetmd5 = "00000000000000000000000000000000"
            else:
                returncode = proc.returncode
                exitcode = returncode
                stderr = "타겟 해시 생성 실패: {0}: {1}".format(
                    jdbcutil.hivetablename(connection, table), out[1].decode("utf-8"))
                if exit:
                    lock.acquire()
                    print(stderr, flush=True)
                    lock.release()
    if exitcode == 0:
        if exit:
            lock.acquire()
            if sourcemd5 == targetmd5:
                print("테이블 해시 일치: {0}".format(
                    jdbcutil.hivetablename(connection, table)), flush=True)
            else:
                print("테이블 해시 불일치: {0}: {1}:{2}".format(jdbcutil.hivetablename(
                    connection, table), sourcemd5, targetmd5), flush=True)
            lock.release()
        if partition is not None:
            stdout = json.dumps(
                {"MD5": {partition: [sourcemd5, targetmd5]}}, ensure_ascii=False)
        else:
            stdout = json.dumps(
                {"MD5": [sourcemd5, targetmd5]}, ensure_ascii=False)
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
            run(None, None, conf, batch_date, connection, table, None, True)


def main():
    if len(sys.argv) < 3:
        print("사용법: hiveverify {배치기준시간} {연결관리번호} [테이블명]...", flush=True)
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
