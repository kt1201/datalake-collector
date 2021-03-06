#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys

import csvutil
import jdbc2json
import jdbcconf


def insert(jdbc, arg, tablename):
    try:
        meta = csvutil.readfile(
            "cfg/IM-{0}/DBMS-{0}-{1}.csv".format(arg, tablename))
        jdbc.execute_update(
            "DELETE FROM {0} WHERE CNNC_MANAGE_NO = ?".format(tablename), [arg])
        statement = jdbc.statement("INSERT INTO {0} ( {1} ) VALUES ( {2} )".format(
            tablename, ", ".join(meta[0]),  ", ".join(["?" for _ in range(len(meta[0]))])))
        meta.pop(0)
        while len(meta) > 0:
            statement.execute_list(meta[0:1000])
            meta = meta[1000:]
    except Exception as e:
        print("Exception: {0}".format(e), flush=True)


def run(jdbc, arg):
    insert(jdbc, arg, "T_RULE_META_0001")
    insert(jdbc, arg, "T_RULE_META_0002")
    insert(jdbc, arg, "T_RULE_META_0003")


def buildrule(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    jdbc = jdbc2json.new(conf["repository"])
    for arg in argv:
        try:
            jdbc.setautocommit(False)
            run(jdbc, arg)
            jdbc.commit()
            print("Rule 복구 성공: {0}".format(arg), flush=True)
        except Exception as e:
            jdbc.rollback()
            print("Rule 복구 실패: {0}: {1}".format(arg, e), flush=True)


def main():
    if len(sys.argv) < 2:
        print("사용법: restorerule {연결관리번호}...", flush=True)
        sys.exit(1)
    buildrule(sys.argv[1:])


if __name__ == "__main__":
    main()
