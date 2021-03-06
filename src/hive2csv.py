#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys

import jdbcconf
import jdbcutil


def hiveselect(batch_date, connection, table):
    kwds = {"DBTYPE": connection["DB_TY_NM"], "BATCH_DATE": batch_date,
            "APD_CHK_COL": table["APD_CHK_COL"]}
    target_list = []
    where_clause = []
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is not None:
            columnname = jdbcutil.hivecolumnname(column)
            target_list.append(columnname)
        else:
            where_clause.append(
                column["HIVE_COL_NM"] + " = " + jdbcutil.jdbcexpression(column, kwds))
    query = "SELECT {0} FROM {1}".format(
        ", ".join(target_list), jdbcutil.hivetablename(connection, table))
    if len(where_clause) > 0:
        query = query + " WHERE " + " AND ".join(where_clause)
    return query


def run(conf, batch_date, connection, table, filename, exit=False):
    command = jdbcconf.javacommand(conf["hive"], connection)
    command.append("kr.co.penta.datalake.jdbc2csv.Application")
    command.append(hiveselect(batch_date, connection, table))
    command.append(filename)
    proc = subprocess.Popen(command, stdin=None)
    out = proc.communicate()
    if exit:
        sys.exit(proc.returncode)
    else:
        return proc.returncode


def jdbc2csv(argv):
    cwd = os.getcwd()
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = argv[0]
    connection = jdbcconf.connection(argv[1])
    table = jdbcconf.table(connection, argv[2])
    filename = argv[3]
    if not filename.startswith(os.path.sep):
        filename = cwd + os.path.sep + filename
    run(conf, batch_date, connection, table, filename, True)


def main():
    if len(sys.argv) < 5:
        print("사용법: hive2csv {배치기준시간} {연결관리번호} {테이블명} {파일명}", flush=True)
        sys.exit(1)
    jdbc2csv(sys.argv[1:])


if __name__ == "__main__":
    main()
