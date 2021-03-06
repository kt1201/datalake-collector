#!/usr/bin/python3
# -*- coding: utf-8 -*-

import contextlib
import os
import sys

import csvutil
import jdbc2json
import jdbcconf


def getheaders(header):
    headers = [None, None, None, None]
    for index in range(len(header)):
        if header[index] == "CNNC_MANAGE_NO":
            headers[0] = index
        if header[index] == "TABLE_ENG_NM":
            headers[1] = index
        if header[index] == "DB_TABLE_ID":
            headers[2] = index
        if header[index] == "DB_1_SERVICE_NM":
            headers[3] = index
    return headers


def run(jdbc, arg):
    dbtableid = {}
    dbms = jdbc.execute_query(
        "SELECT * FROM T_RULE_META_0001 WHERE CNNC_MANAGE_NO = ? ORDER BY CNNC_MANAGE_NO", [arg], csv=True, header=True)
    headers = getheaders(dbms[0])
    if headers[3] is not None:
        dbms[0][headers[3]] = "DB_SERVICE_NM"
    if len(dbms) <= 1:
        print("시스템을 찾을 수 없음: {0}".format(arg))
    with contextlib.suppress(FileExistsError):
        os.mkdir("cfg/IM-{0}".format(arg))
    csvutil.writefile(
        "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0001.csv".format(arg), dbms)
    dbms = jdbc.execute_query(
        "SELECT * FROM T_RULE_META_0002 WHERE CNNC_MANAGE_NO = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM", [arg], csv=True, header=True)
    headers = getheaders(dbms[0])
    if headers[2] is not None:
        dbms[0].pop(headers[2])
        for record in dbms[1:]:
            dbtableid[record[headers[2]]] = {
                "CNNC_MANAGE_NO": None, "TABLE_ENG_NM": None}
            if headers[0] is not None:
                dbtableid[record[headers[2]]
                          ]["CNNC_MANAGE_NO"] = record[headers[0]]
            if headers[1] is not None:
                dbtableid[record[headers[2]]
                          ]["TABLE_ENG_NM"] = record[headers[1]]
            record.pop(headers[2])
    csvutil.writefile(
        "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0002.csv".format(arg), dbms)
    if headers[2] is None:
        dbms = jdbc.execute_query(
            "SELECT * FROM T_RULE_META_0003 WHERE CNNC_MANAGE_NO = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM, DB_TABLE_ATRB_SN", [arg], csv=True, header=True)
    else:
        dbms = jdbc.execute_query(
            "SELECT * FROM T_RULE_META_0003 WHERE DB_TABLE_ID LIKE ? ORDER BY DB_TABLE_ID, DB_TABLE_ATRB_SN", [arg+"-%"], csv=True, header=True)
    headers = getheaders(dbms[0])
    if headers[2] is not None:
        dbms[0].pop(headers[2])
        if headers[1] is None:
            dbms[0].insert(0, "TABLE_ENG_NM")
        if headers[0] is None:
            dbms[0].insert(0, "CNNC_MANAGE_NO")
        for record in dbms[1:]:
            id = dbtableid[record[headers[2]]]
            record.pop(headers[2])
            if headers[1] is None:
                record.insert(0, id["TABLE_ENG_NM"])
            if headers[0] is None:
                record.insert(0, id["CNNC_MANAGE_NO"])
    csvutil.writefile(
        "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0003.csv".format(arg), dbms)


def backuprule(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    for arg in argv:
        jdbc = jdbc2json.new(conf["repository"])
        try:
            run(jdbc, arg)
            print("Rule 백업 성공: {0}".format(arg), flush=True)
        except Exception as e:
            print("Rule 백업 실패: {0}: {1}".format(arg, e), flush=True)


def main():
    if len(sys.argv) < 2:
        print("사용법: backuprule {연결관리번호}...", flush=True)
        sys.exit(1)
    backuprule(sys.argv[1:])


if __name__ == "__main__":
    main()
