#!/usr/bin/python3
# -*- coding: utf-8 -*-

import contextlib
import glob
import os
import sys

import jdbc2json
import jdbcconf

conf = {}
connection = {}
tables = {}
columns = {}


def queryconnection(jdbc, cnnc_manage_no):
    try:
        records = jdbc.execute_query(
            "SELECT * FROM T_RULE_META_0001 WHERE CNNC_MANAGE_NO = ?", [cnnc_manage_no])
        if jdbc.count() <= 0:
            print("시스템 없음: {0}".format(cnnc_manage_no), flush=True)
            sys.exit(1)
        if jdbc.count() > 1:
            print(records, flush=True)
            sys.exit(1)
        return records[0]
    except Exception as e:
        print(e, flush=True)
    sys.exit(1)


def querytables(jdbc, cnnc_manage_no):
    try:
        result = {}
        records = jdbc.execute_query(
            "SELECT * FROM T_RULE_META_0002 WHERE CNNC_MANAGE_NO = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM", [cnnc_manage_no])
        for record in records:
            result[record["TABLE_ENG_NM"]] = record
        return result
    except Exception as e:
        print(e, flush=True)
    sys.exit(1)


def querycolumns(jdbc, cnnc_manage_no):
    try:
        result = {}
        records = jdbc.execute_query(
            "SELECT * FROM T_RULE_META_0003 WHERE CNNC_MANAGE_NO = ? ORDER BY TABLE_ENG_NM, DB_TABLE_ATRB_SN", [cnnc_manage_no])
        if jdbc.error() is not None:
            print(jdbc.error(), flush=True)
            sys.exit(1)
        for record in records:
            if record["TABLE_ENG_NM"] not in result:
                result[record["TABLE_ENG_NM"]] = []
            result[record["TABLE_ENG_NM"]].append(record)
        return result
    except Exception as e:
        print(e, flush=True)
    sys.exit(1)


def initalize():
    global conf, connection, tables, columns
    jdbcconf.home()
    conf = jdbcconf.conf()
    jdbc = jdbc2json.new(conf["repository"])
    connection = queryconnection(jdbc, sys.argv[1])
    tables = querytables(jdbc, connection["CNNC_MANAGE_NO"])
    columns = querycolumns(jdbc, connection["CNNC_MANAGE_NO"])


def main():
    if len(sys.argv) < 2:
        print("사용법: buildcfg {연결관리번호}", flush=True)
        sys.exit(1)
    initalize()
    with contextlib.suppress(FileExistsError):
        os.mkdir("cfg/IM-{0}".format(sys.argv[1]))
    files = glob.glob("cfg/IM-{0}/TAB-{0}-*.json".format(sys.argv[1]))
    for file in files:
        with contextlib.suppress(FileNotFoundError):
            os.remove(file)
    jdbcconf.make_connection_config(conf, connection, tables)
    for key in sorted(tables.keys()):
        jdbcconf.make_table_config(conf, connection, tables[key], columns[key])
        print("설정 파일 생성 성공: {0}".format(
            tables[key]["TABLE_ENG_NM"]), flush=True)


if __name__ == "__main__":
    main()
