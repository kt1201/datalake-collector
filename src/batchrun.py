#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import sys
import threading

import jdbc2json
import jdbcconf

import load000
import load001
import load002
import load003
import load004

conf = {}
batch_date = None
cnnc_mthd_code = None
status = {}
exec_no = None
last_exec_no = None
connections = {}
pool = []
lock = threading.Lock()
exitcode = 0

handlers = {"000": load000, "00": load000, "0": load000,
            "001": load001, "01": load001, "1": load001,
            "002": load002, "02": load002, "2": load002,
            "003": load003, "03": load003, "3": load003,
            "004": load004, "04": load004, "4": load004}


def doclearpartition(table):
    result = False
    try:
        if table["GTHNLDN_MTH_CODE"] in handlers:
            result = handlers[table["GTHNLDN_MTH_CODE"]].clearpartition
    except:
        result = False
    return result


def now():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def divide(n1, n2):
    if n2 == 0.0:
        return 0
    return n1 / n2


def get_status(jdbc):
    global exec_no, exitcode
    query = "SELECT * FROM T_BATCH_STATUS_0001 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_date, cnnc_mthd_code]
    records = None
    try:
        records = jdbc.execute_query(query, binds)
        if len(records) > 1 or len(records) < 0:
            print("테이블 'T_BATCH_STATUS_0001'로부터 조회된 레코드가 {0}건".format(
                len(records)), flush=True)
            exitcode = 1
            finalize()
        if len(records) == 1:
            query = "UPDATE T_BATCH_STATUS_0001 SET EXEC_NO = EXEC_NO + 1, STATUS = ?, ACTUAL_BEGIN_DT = ?, ACTUAL_END_DT = NULL, LAST_EXEC_NO = EXEC_NO, LAST_STATUS = STATUS, LAST_ACTUAL_BEGIN_DT = ACTUAL_BEGIN_DT, LAST_ACTUAL_END_DT = ACTUAL_END_DT WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
            binds = ["EXECUTING", now(), batch_date, cnnc_mthd_code]
            jdbc.execute_update(query, binds)
        else:
            query = "INSERT INTO T_BATCH_STATUS_0001 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, EXEC_NO, STATUS, ACTUAL_BEGIN_DT ) VALUES ( ?, ?, ?, ?, ?, ? )"
            binds = [batch_date, cnnc_mthd_code, "JDBC", 1, "EXECUTING", now()]
            jdbc.execute_update(query, binds)
        query = "SELECT * FROM T_BATCH_STATUS_0001 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
        binds = [batch_date, cnnc_mthd_code]
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
    return records[0]


def get_elapsed(jdbc):
    global exitcode
    elapsed = {}
    query = "SELECT MAX(PLANNED_DT) AS MAX FROM T_BATCH_STATUS_0001 WHERE PLANNED_DT < ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_date, cnnc_mthd_code]
    try:
        records = jdbc.execute_query(query, binds)
        if len(records) != 1:
            print("테이블 'T_BATCH_STATUS_0001'로부터 조회된 레코드가 {0}건".format(
                len(records)), flush=True)
            exitcode = 1
            finalize()
        query = "SELECT CNNC_MANAGE_NO, TABLE_ENG_NM, EXEC_NO, ACTUAL_BEGIN_DT, ACTUAL_END_DT FROM T_BATCH_LOG_0002 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND STATUS = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM, EXEC_NO"
        binds = [records[0]["MAX"], cnnc_mthd_code, "SUCCESS"]
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
    query = "SELECT DISTINCT CNNC_MANAGE_NO, TABLE_ENG_NM FROM T_BATCH_LOG_0002 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND STATUS = ? ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM"
    binds = [batch_date, cnnc_mthd_code, "SUCCESS"]
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


def insert_waiting_list(jdbc):
    global exitcode
    for connection in connections:
        connection = connections[connection]
        query = "INSERT INTO T_BATCH_LOG_0001 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, EXEC_NO, CNNC_MANAGE_NO, STATUS ) VALUES ( ?, ?, ?, ?, ?, ? )"
        binds = [batch_date, cnnc_mthd_code, "JDBC", exec_no,
                 connection["CNNC_MANAGE_NO"], connection["status"]]
        try:
            jdbc.execute_update(query, binds)
            for table in connection["tables"]:
                table = connection["tables"][table]
                query = "INSERT INTO T_BATCH_LOG_0002 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, EXEC_NO, CNNC_MANAGE_NO, TABLE_ENG_NM, STATUS ) VALUES ( ?, ?, ?, ?, ?, ?, ? )"
                binds = [batch_date, cnnc_mthd_code, "JDBC", exec_no,
                         connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]]
                if table["status"] == "SUCCESS":
                    binds.append("SKIPWAITING")
                else:
                    binds.append(table["status"])
                jdbc.execute_update(query, binds)
        except Exception as e:
            print("{0}".format(e), flush=True)
            exitcode = 1
            finalize()


def initalize():
    global conf, batch_date, cnnc_mthd_code, status, exec_no, last_exec_no, connections
    jdbcconf.home()
    conf = jdbcconf.conf()
    batch_date = sys.argv[1]
    cnnc_mthd_code = sys.argv[2]
    jdbc = jdbc2json.new(conf["repository"])
    status = get_status(jdbc)
    exec_no = status["EXEC_NO"]
    last_exec_no = status["LAST_EXEC_NO"]
    elapsed = get_elapsed(jdbc)
    skip = get_skip(jdbc)
    for connection in sys.argv[3:]:
        connections[connection] = jdbcconf.connection(connection)
        if connections[connection]["CNTC_MTHD_CODE"] not in conf["cntc_mthd_nm"]:
            print("CNTC_MTHD_NM 없음: {0}: {1}".format(
                connection, connections[connection]["CNTC_MTHD_CODE"]), file=sys.stderr, flush=True)
            del connections[connection]
            continue
        if connections[connection]["CNTC_MTHD_CODE"] != cnnc_mthd_code:
            print("CNTC_MTHD_NM 불일치: {0}: {1}".format(
                connection, connections[connection]["CNTC_MTHD_CODE"]), file=sys.stderr, flush=True)
            del connections[connection]
            continue
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
    insert_waiting_list(jdbc)


def finalize():
    jdbc = jdbc2json.new(conf["repository"])
    query = "UPDATE T_BATCH_STATUS_0001 SET STATUS = ?, ACTUAL_END_DT = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = ["SUCCESS", now(), batch_date, cnnc_mthd_code]
    if exitcode != 0:
        binds[0] = "FAILURE"
    try:
        jdbc.execute_update(query, binds)
    except Exception as e:
        print("{0}".format(e), flush=True)
        sys.exit(1)
    if exitcode != 0:
        sys.exit(exitcode)


def run(jdbc, hive, conf, batch_date, connection, table):
    global exitcode
    stdout = None
    stderr = None
    if table["status"] != "SUCCESS":
        table["status"] = "EXECUTING"
        query = "UPDATE T_BATCH_LOG_0002 SET STATUS = ?, ACTUAL_BEGIN_DT = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND EXEC_NO = ? AND CNNC_MANAGE_NO = ? AND TABLE_ENG_NM = ?"
        binds = [table["status"], now(), batch_date, cnnc_mthd_code,
                 exec_no, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]]
        try:
            jdbc.execute_update(query, binds)
            if table["GTHNLDN_MTH_CODE"] in handlers:
                result = handlers[table["GTHNLDN_MTH_CODE"]].run(
                    jdbc, hive, conf, batch_date, connection, table)
                if result[0] == 0:
                    table["status"] = "SUCCESS"
                    print("테이블 불러오기 성공 {0}.{1}".format(
                        connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]), flush=True)
                else:
                    exitcode = result[0]
                    table["status"] = "FAILURE"
                    print("테이블 불러오기 실패 {0}.{1}".format(
                        connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]), flush=True)
                stdout = result[1]
                stderr = result[2]
            else:
                exitcode = 1
                table["status"] = "FAILURE"
                stderr = "핸들러({0})가 없음 {1}.{2}".format(
                    table["GTHNLDN_MTH_CODE"], connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"])
                print(stderr, flush=True)
            query = "UPDATE T_BATCH_LOG_0002 SET STATUS = ?, ACTUAL_END_DT = ?, STDOUT = ?, STDERR = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND EXEC_NO = ? AND CNNC_MANAGE_NO = ? AND TABLE_ENG_NM = ?"
            binds = [table["status"], now(), stdout, stderr, batch_date, cnnc_mthd_code,
                     exec_no, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]]
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("{0}".format(e), flush=True)
            exitcode = 1
            finalize()
    else:
        table["status"] = "SKIP"
        print("테이블 불러오기 건너뛰기 {0}.{1}".format(
            connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]), flush=True)
        query = "UPDATE T_BATCH_LOG_0002 SET STATUS = ?, ACTUAL_BEGIN_DT = ?, ACTUAL_END_DT = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND EXEC_NO = ? AND CNNC_MANAGE_NO = ? AND TABLE_ENG_NM = ?"
        binds = [table["status"], now(), now(), batch_date, cnnc_mthd_code,
                 exec_no, connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]]
        try:
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("{0}".format(e), flush=True)
            exitcode = 1
            finalize()
    lock.acquire()
    if table["status"] == "SUCCESS":
        connection["table_success"] = connection["table_success"] + 1
    elif table["status"] == "FAILURE":
        connection["table_failure"] = connection["table_failure"] + 1
    elif table["status"] == "SKIP":
        connection["table_skip"] = connection["table_skip"] + 1
    else:
        print("테이블 종료 상태 이상: {0}".format(table["status"]), flush=True)
        exitcode = 1
        finalize()
    if connection["table_success"] + connection["table_failure"] + connection["table_skip"] >= connection["table_count"]:
        connection["status"] = "SUCCESS"
        if connection["table_failure"] > 0:
            connection["status"] = "FAILURE"
        query = "UPDATE T_BATCH_LOG_0001 SET STATUS = ?, ACTUAL_END_DT = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND EXEC_NO = ? AND CNNC_MANAGE_NO = ?"
        binds = [connection["status"], now(), batch_date, cnnc_mthd_code,
                 exec_no, connection["CNNC_MANAGE_NO"]]
        try:
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("{0}".format(e), flush=True)
            exitcode = 1
            finalize()
    lock.release()


def worker():
    global connections, exitcode
    jdbc = jdbc2json.new(conf["repository"])
    hive = jdbc2json.new(conf["hive"])
    while len(connections) > 0:
        connection = None
        table = None
        lock.acquire()
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
            if connection["status"] == "WAITING":
                connection["status"] = "EXECUTING"
                query = "UPDATE T_BATCH_LOG_0001 SET STATUS = ?, ACTUAL_BEGIN_DT = ? WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ? AND EXEC_NO = ? AND CNNC_MANAGE_NO = ?"
                binds = [connection["status"], now(), batch_date,
                         cnnc_mthd_code, exec_no, connection["CNNC_MANAGE_NO"]]
                try:
                    jdbc.execute_update(query, binds)
                except Exception as e:
                    print("{0}".format(e), flush=True)
                    exitcode = 1
                    finalize()
            key = list(connection["tables"].keys())
            if len(key) > 0:
                table = connection["tables"][key[0]]
                del connection["tables"][key[0]]
                connection["elapsed"] = connection["elapsed"] + \
                    table["estimated"]
            if len(key) == 1:
                del connections[connection["CNNC_MANAGE_NO"]]
        lock.release()
        if connection is not None and table is not None:
            run(jdbc, hive, conf, batch_date, connection, table)


def main():
    if len(sys.argv) < 4:
        print("사용법: batchrun {배치기준시간} {연계방식코드} {연결관리번호}...", flush=True)
        sys.exit(1)
    initalize()
    if conf["threads"] > 1:
        for index in range(conf["threads"]):
            pool.append(
                threading.Thread(target=worker, name="Worker {0}".format(index)))
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join()
    else:
        worker()
    finalize()
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
