#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import sys

import jdbc2json
import jdbcconf

conf = {}
batch_date = None
connections = {}
pool = []
exitcode = 0


def divide(n1, n2):
    if n2 == 0.0:
        return 0
    return n1 / n2


def get_elapsed(jdbc):
    global exitcode
    elapsed = {}
    query = "SELECT MAX(PLANNED_DT) AS MAX FROM T_BATCH_STATUS_0001 WHERE PLANNED_DT < ?"
    binds = [batch_date]
    records = None
    try:
        records = jdbc.execute_query(query, binds)
        if len(records) != 1:
            print("테이블 'T_BATCH_STATUS_0001'로부터 조회된 레코드가 {0}건".format(
                len(records)), flush=True)
            exitcode = 1
            finalize()
    except Exception as e:
        print("{0}".format(e), flush=True)
        exitcode = 1
        finalize()
    query = "SELECT CNNC_MANAGE_NO, TABLE_ENG_NM, EXEC_NO, ACTUAL_BEGIN_DT, ACTUAL_END_DT FROM T_BATCH_LOG_0002 WHERE PLANNED_DT = ? AND STATUS = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM, EXEC_NO"
    binds = [records[0]["MAX"], "SUCCESS"]
    try:
        records = jdbc.execute_query(query, binds)
        for record in records:
            if record["CNNC_MANAGE_NO"] not in elapsed:
                elapsed[record["CNNC_MANAGE_NO"]] = {}
            if record["TABLE_ENG_NM"] not in elapsed[record["CNNC_MANAGE_NO"]]:
                elapsed[record["CNNC_MANAGE_NO"]
                        ][record["TABLE_ENG_NM"]] = None
            actual_begin_dt = datetime.datetime.strptime(
                record["ACTUAL_BEGIN_DT"], "%Y%m%d%H%M%S")
            actual_end_dt = datetime.datetime.strptime(
                record["ACTUAL_END_DT"], "%Y%m%d%H%M%S")
            elapsed[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]
                                              ] = (actual_end_dt - actual_begin_dt).total_seconds()
    except Exception as e:
        print("{0}".format(e), flush=True)
        exitcode = 1
        finalize()
    return elapsed


def get_skip(jdbc):
    global exitcode
    skip = {}
    query = "SELECT DISTINCT CNNC_MANAGE_NO, TABLE_ENG_NM FROM T_BATCH_LOG_0002 WHERE PLANNED_DT = ? AND STATUS = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM"
    binds = [batch_date, "SUCCESS"]
    try:
        records = jdbc.execute_query(query, binds)
        for record in records:
            if record["CNNC_MANAGE_NO"] not in skip:
                skip[record["CNNC_MANAGE_NO"]] = {}
            if record["TABLE_ENG_NM"] not in skip[record["CNNC_MANAGE_NO"]]:
                skip[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]] = None
            skip[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]] = "SUCCESS"
    except Exception as e:
        print("{0}".format(e), flush=True)
        exitcode = 1
        finalize()
    return skip


def initalize():
    global conf, batch_date, connections
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = sys.argv[1]
    jdbc = jdbc2json.new(conf["repository"])
    elapsed = get_elapsed(jdbc)
    skip = get_skip(jdbc)
    for connection in sys.argv[2:]:
        connections[connection] = jdbcconf.connection(connection)
        tables = list(jdbcconf.tables(
            conf, connections[connection], []).values())
        estimated = 0.0
        for table in tables:
            table["estimated"] = 1.0
            if connection in elapsed:
                if table["TABLE_ENG_NM"] in elapsed[connection]:
                    table["estimated"] = elapsed[connection][table["TABLE_ENG_NM"]]
                    if table["estimated"] <= 1.0:
                        table["estimated"] = 1.0
            table["status"] = "WAITING"
            if connection in skip:
                if table["TABLE_ENG_NM"] in skip[connection]:
                    table["status"] = skip[connection][table["TABLE_ENG_NM"]]
            estimated = estimated + table["estimated"]
        tables.sort(key=lambda d: d["estimated"], reverse=True)
        connections[connection]["tables"] = {}
        for table in tables:
            connections[connection]["tables"][table["TABLE_ENG_NM"]] = table
        connections[connection]["estimated"] = estimated
        connections[connection]["elapsed"] = 0.0
        connections[connection]["status"] = "WAITING"
        connections[connection]["table_success"] = 0
        connections[connection]["table_failure"] = 0
        connections[connection]["table_skip"] = 0
        connections[connection]["table_count"] = len(
            connections[connection]["tables"])
        if connections[connection]["table_count"] <= 0:
            del connections[connection]


def finalize():
    if exitcode != 0:
        sys.exit(exitcode)


def run():
    global connections, exitcode
    while len(connections) > 0:
        connection = None
        table = None
        for cnnc_manage_no in connections.keys():
            connection2 = connections[cnnc_manage_no]
            if connection is not None:
                if divide(connection["elapsed"], connection["estimated"]) >= divide(connection2["elapsed"], connection2["estimated"]):
                    if divide(connection["elapsed"], connection["estimated"]) > divide(connection2["elapsed"], connection2["estimated"]):
                        connection = connection2
                    elif connection["estimated"] < connection2["estimated"]:
                        connection = connection2
            else:
                connection = connection2
        if connection is not None:
            key = list(connection["tables"].keys())
            if len(key) > 0:
                table = connection["tables"][key[0]]
                del connection["tables"][key[0]]
                connection["elapsed"] = connection["elapsed"] + \
                    table["estimated"]
            if len(key) == 1:
                del connections[connection["CNNC_MANAGE_NO"]]
        if connection is not None and table is not None:
            print("테이블 {0}.{1}: {2}".format(
                connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"], table["status"]), flush=True)
            if table["status"] != "SUCCESS":
                exitcode = 1


def main():
    if len(sys.argv) < 3:
        print("사용법: batchcheck {배치기준시간} {연결관리번호}...", flush=True)
        sys.exit(1)
    initalize()
    run()
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
