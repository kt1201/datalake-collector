#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import json
import sys

import jdbc2json
import jdbcconf


def batch_result(batch_date, cnnc_mthd_code):

    jdbcconf.home()
    conf = jdbcconf.conf()
    jdbc = jdbc2json.new(conf["repository"])
    hive = jdbc2json.new(conf["hive"])

    cntc_mthd_nm_dict = conf["cntc_mthd_nm"]

    today = datetime.datetime.strptime(batch_date, "%Y%m%d%H%M%S")
    yesterday = today - datetime.timedelta(days=1)

    batch_status = {"CNTC_MTHD_CODE": cnnc_mthd_code, "PLANNED_DT": batch_date}
    records = None
    log1 = {}
    query = "SELECT CNTC_MTHD_CODE, CNTC_MTHD_NM, PLANNED_DT, STATUS FROM T_BATCH_STATUS_0001 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_date, cnnc_mthd_code]
    try:
        batch_status_list = jdbc.execute_query(query, binds)
        if jdbc.count() != 1:
            print("조회된 레코드수가 1건이 아님: {0}".format(jdbc.count()), flush=True)
            sys.exit(1)
    except Exception as e:
        print("배치 상태 테이블 SELECT 실패", flush=True)
        print("{0}".format(e), flush=True)
        sys.exit(1)
    batch_status = batch_status_list[0]
    if batch_status["STATUS"] != "SUCCESS":
        print("실패한 배치: {0}: {1}: {2}".format(
            batch_status["PLANNED_DT"], batch_status["CNTC_MTHD_NM"], batch_status["STATUS"]), flush=True)

    query = "SELECT CNNC_MANAGE_NO, EXEC_NO, STDOUT, STDERR FROM T_BATCH_LOG_0001 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?  ORDER BY CNNC_MANAGE_NO, EXEC_NO"
    binds = [batch_status["PLANNED_DT"], batch_status["CNTC_MTHD_CODE"]]
    try:
        records = jdbc.execute_query(query, binds)
        for record in records:
            log1[record["CNNC_MANAGE_NO"]] = {
                "STDOUT": record["STDOUT"], "STDERR": record["STDERR"]}
    except Exception as e:
        print("시스템별 로그 테이블 SELECT 실패", flush=True)
        print("{0}".format(e), flush=True)
        sys.exit(1)

    log2 = {}
    query = "SELECT CNNC_MANAGE_NO, TABLE_ENG_NM, ACCUMULATED_RECORDS FROM T_BATCH_RESULT_0003 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?  ORDER BY CNNC_MANAGE_NO, TABLE_ENG_NM"
    binds = [yesterday.strftime("%Y%m%d"), batch_status["CNTC_MTHD_CODE"]]
    try:
        records = jdbc.execute_query(query, binds)
        for record in records:
            if record["CNNC_MANAGE_NO"] not in log2:
                log2[record["CNNC_MANAGE_NO"]] = {}
            log2[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]] = {
                "ACCUMULATED_RECORDS": record["ACCUMULATED_RECORDS"]}
    except Exception as e:
        print("테이블별 로그 테이블 SELECT 실패", flush=True)
        print("{0}".format(e), flush=True)
        sys.exit(1)

    query = ["SELECT A.PLANNED_DT, A.CNTC_MTHD_CODE, A.CNTC_MTHD_NM, A.CNNC_MANAGE_NO, A.TABLE_ENG_NM, A.EXEC_NO, A.STATUS, A.ACTUAL_BEGIN_DT, A.ACTUAL_END_DT, A.STDOUT, A.STDERR,",
             "B.TABLE_KOREAN_NM, B.PART_COLS, B.GTHNLDN_MTH_CODE, B.HIVE_TABLE_NM, C.SYS_NM, C.HIVE_DB_NM",
             "FROM T_BATCH_LOG_0002 A, T_RULE_META_0002 B, T_RULE_META_0001 C",
             "WHERE A.CNTC_MTHD_CODE = ? AND A.PLANNED_DT = ? AND A.CNNC_MANAGE_NO = B.CNNC_MANAGE_NO AND A.TABLE_ENG_NM = B.TABLE_ENG_NM AND B.CNNC_MANAGE_NO = C.CNNC_MANAGE_NO",
             "ORDER BY A.CNNC_MANAGE_NO, A.TABLE_ENG_NM, A.EXEC_NO"]
    binds = [batch_status["CNTC_MTHD_CODE"], batch_status["PLANNED_DT"]]
    if batch_status["CNTC_MTHD_CODE"] == "04":
        query = ["SELECT A.PLANNED_DT, '04' AS CNTC_MTHD_CODE, A.CNTC_MTHD_NM, A.CNNC_MANAGE_NO, A.TABLE_ENG_NM, A.EXEC_NO, A.STATUS, A.ACTUAL_BEGIN_DT, A.ACTUAL_END_DT, A.STDOUT, A.STDERR,",
                 "B.TABLE_KOREAN_NM, B.PART_COLS, B.GTHNLDN_MTH_CODE, B.HIVE_TABLE_NM, C.SYS_NM, C.HIVE_DB_NM",
                 "FROM T_BATCH_LOG_0002 A, T_RULE_META_0002 B, T_RULE_META_0001 C",
                 "WHERE A.CNTC_MTHD_CODE = ? AND A.PLANNED_DT = ? AND A.CNNC_MANAGE_NO = ? AND A.CNNC_MANAGE_NO = B.CNNC_MANAGE_NO AND A.TABLE_ENG_NM = B.TABLE_ENG_NM AND B.CNNC_MANAGE_NO = C.CNNC_MANAGE_NO",
                 "ORDER BY A.CNNC_MANAGE_NO, A.TABLE_ENG_NM, A.EXEC_NO"]
        binds = ["02", batch_status["PLANNED_DT"], "api_apilist_001"]
    try:
        records = jdbc.execute_query(" ".join(query), binds)
        index = 1
        while index < len(records):
            if records[index-1]["CNNC_MANAGE_NO"] == records[index]["CNNC_MANAGE_NO"] and records[index-1]["TABLE_ENG_NM"] == records[index]["TABLE_ENG_NM"]:
                if records[index-1]["STATUS"] == "SUCCESS":
                    del records[index]
                else:
                    del records[index-1]
            else:
                index = index + 1
    except Exception as e:
        print("테이블별 배치 결과 SELECT 실패", flush=True)
        print("{0}".format(e), flush=True)
        sys.exit(1)

    query = "DELETE FROM T_BATCH_RESULT_0003 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_status["PLANNED_DT"][0:8], batch_status["CNTC_MTHD_CODE"], ]
    try:
        jdbc.execute_update(query, binds)
    except Exception as e:
        print("테이블별 결과 테이블 DELETE 실패", flush=True)
        print("{0}".format(e), flush=True)

    result3 = []
    for record in records:
        if record["STATUS"] != "SUCCESS":
            if record["STDOUT"] is None or len(record["STDOUT"]) <= 0:
                if record["CNNC_MANAGE_NO"] in log1:
                    record["STDOUT"] = log1[record["CNNC_MANAGE_NO"]]["STDOUT"]
            if record["STDERR"] is None or len(record["STDERR"]) <= 0:
                if record["CNNC_MANAGE_NO"] in log1:
                    record["STDERR"] = log1[record["CNNC_MANAGE_NO"]]["STDERR"]
        try:
            std_out = json.loads(record["STDOUT"])
        except:
            std_out = {"Records": None}
        record["PLANNED_DT"] = record["PLANNED_DT"][0:8]
        if record["CNTC_MTHD_CODE"] in cntc_mthd_nm_dict:
            record["CNTC_MTHD_NM"] = cntc_mthd_nm_dict[record["CNTC_MTHD_CODE"]]
        record["ELAPSED"] = None
        if record["ACTUAL_BEGIN_DT"] is not None and len(record["ACTUAL_BEGIN_DT"]) > 0 and record["ACTUAL_END_DT"] is not None and len(record["ACTUAL_END_DT"]) > 0:
            actual_begin_dt = datetime.datetime.strptime(
                record["ACTUAL_BEGIN_DT"], "%Y%m%d%H%M%S")
            actual_end_dt = datetime.datetime.strptime(
                record["ACTUAL_END_DT"], "%Y%m%d%H%M%S")
            record["ELAPSED"] = (
                actual_end_dt - actual_begin_dt).total_seconds()
        record["RECORDS"] = None
        if "Records" in std_out:
            record["RECORDS"] = std_out["Records"]
        record["ACCUMULATED_RECORDS"] = None
        tablename = record["HIVE_TABLE_NM"]
        if tablename is None or len(tablename) <= 0:
            tablename = record["TABLE_ENG_NM"]
        hivequery = "SELECT COUNT(*) AS CNT FROM {0}.{1}".format(
            record["HIVE_DB_NM"], tablename)
        try:
            hiveresult = hive.execute_query(hivequery)
            record["ACCUMULATED_RECORDS"] = hiveresult[0]["CNT"]
        except Exception as e:
            print("{0}".format(e), flush=True)
            record["ACCUMULATED_RECORDS"] = None
        record["VERIFY"] = None
        if "MD5" in std_out:
            if isinstance(std_out["MD5"], list):
                if len(std_out["MD5"]) >= 2:
                    record["VERIFY"] = "SUCCESS"
                    if std_out["MD5"][0] != std_out["MD5"][1]:
                        record["VERIFY"] = "FAILURE"
            if isinstance(std_out["MD5"], dict):
                record["VERIFY"] = "SUCCESS"
                for md5_key in std_out["MD5"].keys():
                    if len(std_out["MD5"][md5_key]) >= 2:
                        if std_out["MD5"][md5_key][0] != std_out["MD5"][md5_key][1]:
                            record["VERIFY"] = "FAILURE"
        record["INCREASED_ACCUMULATED_RECORDS"] = record["ACCUMULATED_RECORDS"]
        if record["CNNC_MANAGE_NO"] in log2:
            if record["TABLE_ENG_NM"] in log2[record["CNNC_MANAGE_NO"]]:
                if "ACCUMULATED_RECORDS" in log2[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]]:
                    if record["ACCUMULATED_RECORDS"] is not None and log2[record["CNNC_MANAGE_NO"]][record["TABLE_ENG_NM"]]["ACCUMULATED_RECORDS"] is not None:
                        record["INCREASED_ACCUMULATED_RECORDS"] = record["ACCUMULATED_RECORDS"] - \
                            log2[record["CNNC_MANAGE_NO"]
                                 ][record["TABLE_ENG_NM"]]["ACCUMULATED_RECORDS"]
                    else:
                        record["INCREASED_ACCUMULATED_RECORDS"] = None
        query = "INSERT INTO T_BATCH_RESULT_0003 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, CNNC_MANAGE_NO, SYS_NM, TABLE_ENG_NM, TABLE_KOREAN_NM, PART_COL_NM, STATUS, ACTUAL_BEGIN_DT, ACTUAL_END_DT, ELAPSED, RECORDS, ACCUMULATED_RECORDS, INCREASED_ACCUMULATED_RECORDS, VERIFY, STDOUT, STDERR ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
        binds = [record["PLANNED_DT"], record["CNTC_MTHD_CODE"], record["CNTC_MTHD_NM"], record["CNNC_MANAGE_NO"], record["SYS_NM"], record["TABLE_ENG_NM"], record["TABLE_KOREAN_NM"], record["PART_COLS"], record["STATUS"],
                 record["ACTUAL_BEGIN_DT"], record["ACTUAL_END_DT"], record["ELAPSED"], record["RECORDS"], record["ACCUMULATED_RECORDS"], record["INCREASED_ACCUMULATED_RECORDS"], record["VERIFY"], record["STDOUT"], record["STDERR"]]
        result3.append(record)
        try:
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("테이블별 결과 테이블 INSERT 실패: {0}: {1}: {2}".format(
                record["CNTC_MTHD_CODE"], record["CNNC_MANAGE_NO"], record["TABLE_ENG_NM"]), flush=True)
            print("{0}".format(e), flush=True)

    query = "DELETE FROM T_BATCH_RESULT_0002 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_status["PLANNED_DT"][0:8], batch_status["CNTC_MTHD_CODE"]]
    try:
        jdbc.execute_update(query, binds)
    except Exception as e:
        print("시스템별 결과 테이블 DELETE 실패", flush=True)
        print("{0}".format(e), flush=True)

    result2 = []
    planned_dt = None
    cntc_mthd_code = None
    cntc_mthd_nm = None
    cnnc_manage_no = None
    sys_nm = None
    status = None
    actual_begin_dt = None
    actual_end_dt = None
    records = None
    accumulated_records = None
    increased_accumulated_records = None
    verify = None
    for result in result3:
        if cnnc_manage_no != result["CNNC_MANAGE_NO"]:
            if cnnc_manage_no is not None:
                elapsed = None
                if actual_begin_dt is not None and actual_end_dt is not None:
                    actual_begin_date = datetime.datetime.strptime(
                        actual_begin_dt, "%Y%m%d%H%M%S")
                    actual_end_date = datetime.datetime.strptime(
                        actual_end_dt, "%Y%m%d%H%M%S")
                    elapsed = (actual_end_date -
                               actual_begin_date).total_seconds()
                query = "INSERT INTO T_BATCH_RESULT_0002 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, CNNC_MANAGE_NO, SYS_NM, STATUS, ACTUAL_BEGIN_DT, ACTUAL_END_DT, ELAPSED, RECORDS, ACCUMULATED_RECORDS, INCREASED_ACCUMULATED_RECORDS, VERIFY ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
                binds = [planned_dt, cntc_mthd_code, cntc_mthd_nm, cnnc_manage_no, sys_nm, status, actual_begin_dt,
                         actual_end_dt, elapsed, records, accumulated_records, increased_accumulated_records, verify]
                record = {"PLANNED_DT": planned_dt, "CNTC_MTHD_CODE": cntc_mthd_code, "CNTC_MTHD_NM": cntc_mthd_nm, "CNNC_MANAGE_NO": cnnc_manage_no, "SYS_NM": sys_nm, "STATUS": status, "ACTUAL_BEGIN_DT": actual_begin_dt,
                          "ACTUAL_END_DT": actual_end_dt, "ELAPSED": elapsed, "RECORDS": records, "ACCUMULATED_RECORDS": accumulated_records, "INCREASED_ACCUMULATED_RECORDS": increased_accumulated_records, "VERIFY": verify}
                result2.append(record)
                try:
                    jdbc.execute_update(query, binds)
                except Exception as e:
                    print("시스템별 결과 테이블 INSERT 실패: {0}: {1}".format(
                        cntc_mthd_code, cnnc_manage_no), flush=True)
                    print("{0}".format(e), flush=True)
            planned_dt = result["PLANNED_DT"]
            cntc_mthd_code = result["CNTC_MTHD_CODE"]
            cntc_mthd_nm = result["CNTC_MTHD_NM"]
            cnnc_manage_no = result["CNNC_MANAGE_NO"]
            sys_nm = result["SYS_NM"]
            status = "SUCCESS"
            if result["STATUS"] != "SUCCESS":
                status = "FAILURE"
            actual_begin_dt = result["ACTUAL_BEGIN_DT"]
            actual_end_dt = result["ACTUAL_END_DT"]
            records = 0
            if result["RECORDS"] is not None:
                records = records + result["RECORDS"]
            accumulated_records = 0
            if result["ACCUMULATED_RECORDS"] is not None:
                accumulated_records = accumulated_records + \
                    result["ACCUMULATED_RECORDS"]
            increased_accumulated_records = 0
            if result["INCREASED_ACCUMULATED_RECORDS"] is not None:
                increased_accumulated_records = increased_accumulated_records + \
                    result["INCREASED_ACCUMULATED_RECORDS"]
            verify = "SUCCESS"
            if result["VERIFY"] is not None and result["VERIFY"] != "SUCCESS":
                verify = "FAILURE"
        else:
            if result["STATUS"] != "SUCCESS":
                status = "FAILURE"
            if result["ACTUAL_BEGIN_DT"] is not None:
                if actual_begin_dt is not None:
                    if result["ACTUAL_BEGIN_DT"] < actual_begin_dt:
                        actual_begin_dt = result["ACTUAL_BEGIN_DT"]
            else:
                actual_begin_dt = None
            if result["ACTUAL_END_DT"] is not None:
                if actual_end_dt is not None:
                    if result["ACTUAL_END_DT"] > actual_end_dt:
                        actual_end_dt = result["ACTUAL_END_DT"]
            else:
                actual_end_dt = None
            if result["RECORDS"] is not None:
                records = records + result["RECORDS"]
            if result["ACCUMULATED_RECORDS"] is not None:
                accumulated_records = accumulated_records + \
                    result["ACCUMULATED_RECORDS"]
            if result["INCREASED_ACCUMULATED_RECORDS"] is not None:
                increased_accumulated_records = increased_accumulated_records + \
                    result["INCREASED_ACCUMULATED_RECORDS"]
            if result["VERIFY"] is not None and result["VERIFY"] != "SUCCESS":
                verify = "FAILURE"
    if cnnc_manage_no is not None:
        elapsed = None
        if actual_begin_dt is not None and actual_end_dt is not None:
            actual_begin_date = datetime.datetime.strptime(
                actual_begin_dt, "%Y%m%d%H%M%S")
            actual_end_date = datetime.datetime.strptime(
                actual_end_dt, "%Y%m%d%H%M%S")
            elapsed = (actual_end_date - actual_begin_date).total_seconds()
        query = "INSERT INTO T_BATCH_RESULT_0002 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, CNNC_MANAGE_NO, SYS_NM, STATUS, ACTUAL_BEGIN_DT, ACTUAL_END_DT, ELAPSED, RECORDS, ACCUMULATED_RECORDS, INCREASED_ACCUMULATED_RECORDS, VERIFY ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
        binds = [planned_dt, cntc_mthd_code, cntc_mthd_nm, cnnc_manage_no, sys_nm, status, actual_begin_dt,
                 actual_end_dt, elapsed, records, accumulated_records, increased_accumulated_records, verify]
        record = {"PLANNED_DT": planned_dt, "CNTC_MTHD_CODE": cntc_mthd_code, "CNTC_MTHD_NM": cntc_mthd_nm, "CNNC_MANAGE_NO": cnnc_manage_no, "SYS_NM": sys_nm, "STATUS": status, "ACTUAL_BEGIN_DT": actual_begin_dt,
                  "ACTUAL_END_DT": actual_end_dt, "ELAPSED": elapsed, "RECORDS": records, "ACCUMULATED_RECORDS": accumulated_records, "INCREASED_ACCUMULATED_RECORDS": increased_accumulated_records, "VERIFY": verify}
        result2.append(record)
        try:
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("시스템별 결과 테이블 INSERT 실패: {0}: {1}".format(
                cntc_mthd_code, cnnc_manage_no), flush=True)
            print("{0}".format(e), flush=True)

    query = "DELETE FROM T_BATCH_RESULT_0001 WHERE PLANNED_DT = ? AND CNTC_MTHD_CODE = ?"
    binds = [batch_status["PLANNED_DT"][0:8], batch_status["CNTC_MTHD_CODE"]]
    try:
        jdbc.execute_update(query, binds)
    except Exception as e:
        print("연계방식별 결과 테이블 DELETE 실패", flush=True)
        print("{0}".format(e), flush=True)

    planned_dt = None
    cntc_mthd_code = None
    cntc_mthd_nm = None
    status = None
    actual_begin_dt = None
    actual_end_dt = None
    records = None
    accumulated_records = None
    increased_accumulated_records = None
    verify = None
    for result in result2:
        if planned_dt is None:
            planned_dt = result["PLANNED_DT"]
            cntc_mthd_code = result["CNTC_MTHD_CODE"]
            cntc_mthd_nm = result["CNTC_MTHD_NM"]
            status = "SUCCESS"
            if result["STATUS"] != "SUCCESS":
                status = "FAILURE"
            actual_begin_dt = result["ACTUAL_BEGIN_DT"]
            actual_end_dt = result["ACTUAL_END_DT"]
            records = 0
            if result["RECORDS"] is not None:
                records = records + result["RECORDS"]
            accumulated_records = 0
            if result["ACCUMULATED_RECORDS"] is not None:
                accumulated_records = accumulated_records + \
                    result["ACCUMULATED_RECORDS"]
            increased_accumulated_records = 0
            if result["INCREASED_ACCUMULATED_RECORDS"] is not None:
                increased_accumulated_records = increased_accumulated_records + \
                    result["INCREASED_ACCUMULATED_RECORDS"]
            verify = "SUCCESS"
            if result["VERIFY"] is not None and result["VERIFY"] != "SUCCESS":
                verify = "FAILURE"
        else:
            if result["STATUS"] != "SUCCESS":
                status = "FAILURE"
            if result["ACTUAL_BEGIN_DT"] is not None:
                if actual_begin_dt is not None:
                    if result["ACTUAL_BEGIN_DT"] < actual_begin_dt:
                        actual_begin_dt = result["ACTUAL_BEGIN_DT"]
            else:
                actual_begin_dt = None
            if result["ACTUAL_END_DT"] is not None:
                if actual_end_dt is not None:
                    if result["ACTUAL_END_DT"] > actual_end_dt:
                        actual_end_dt = result["ACTUAL_END_DT"]
            else:
                actual_end_dt = None
            if result["RECORDS"] is not None:
                records = records + result["RECORDS"]
            if result["ACCUMULATED_RECORDS"] is not None:
                accumulated_records = accumulated_records + \
                    result["ACCUMULATED_RECORDS"]
            if result["INCREASED_ACCUMULATED_RECORDS"] is not None:
                increased_accumulated_records = increased_accumulated_records + \
                    result["INCREASED_ACCUMULATED_RECORDS"]
            if result["VERIFY"] is not None and result["VERIFY"] != "SUCCESS":
                verify = "FAILURE"
    if planned_dt is not None:
        elapsed = None
        if actual_begin_dt is not None and actual_end_dt is not None:
            actual_begin_date = datetime.datetime.strptime(
                actual_begin_dt, "%Y%m%d%H%M%S")
            actual_end_date = datetime.datetime.strptime(
                actual_end_dt, "%Y%m%d%H%M%S")
            elapsed = (actual_end_date - actual_begin_date).total_seconds()
        query = "INSERT INTO T_BATCH_RESULT_0001 ( PLANNED_DT, CNTC_MTHD_CODE, CNTC_MTHD_NM, STATUS, ACTUAL_BEGIN_DT, ACTUAL_END_DT, ELAPSED, RECORDS, ACCUMULATED_RECORDS, INCREASED_ACCUMULATED_RECORDS, VERIFY ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
        binds = [planned_dt, cntc_mthd_code, cntc_mthd_nm, status, actual_begin_dt, actual_end_dt,
                 elapsed, records, accumulated_records, increased_accumulated_records, verify]
        try:
            jdbc.execute_update(query, binds)
        except Exception as e:
            print("연계방식별 결과 테이블 INSERT 실패: {0}".format(
                cntc_mthd_code), flush=True)
            print("{0}".format(e), flush=True)

    planned_dt = batch_status["PLANNED_DT"][0:8]
    bit = 1
    for key in sorted(cntc_mthd_nm_dict.keys()):
        if cnnc_mthd_code == key:
            break
        bit <<= 1
    count = 0
    while count < 3:
        query = "SELECT FLAG FROM T_BATCH_DATE_0001 WHERE PLANNED_DT = ?"
        binds = [planned_dt]
        try:
            records = jdbc.execute_query(query, binds)
            if len(records) >= 1:
                flag = records[0]["FLAG"]
                query = "UPDATE T_BATCH_DATE_0001 SET FLAG = ? WHERE PLANNED_DT = ? AND FLAG = ?"
                binds = [flag & ~bit, planned_dt, flag]
                try:
                    jdbc.execute_update(query, binds)
                    if jdbc.count() == 1:
                        if (flag & ~bit) == 0:
                            query = "DELETE FROM T_BATCH_DATE_0001 WHERE PLANNED_DT <> ?"
                            binds = [planned_dt]
                            try:
                                jdbc.execute_update(query, binds)
                            except Exception as e:
                                print("배치일자 테이블 DELETE 실패: {0}".format(
                                    cntc_mthd_code), flush=True)
                                print("{0}".format(e), flush=True)
                        break
                except Exception as e:
                    print("배치일자 테이블 UPDATE 실패: {0}".format(
                        cntc_mthd_code), flush=True)
                    print("{0}".format(e), flush=True)
            else:
                flag = 0
                for bit in range(len(cntc_mthd_nm_dict)):
                    flag |= 1 << bit
                query = "INSERT INTO T_BATCH_DATE_0001 ( PLANNED_DT, FLAG ) VALUES ( ?, ? )"
                binds = [planned_dt, flag & ~bit]
                try:
                    jdbc.execute_update(query, binds)
                except Exception as e:
                    print("배치일자 테이블 INSERT 실패: {0}".format(
                        cntc_mthd_code), flush=True)
                    print("{0}".format(e), flush=True)
        except Exception as e:
            print("배치일자 테이블 SELECT 실패: {0}".format(cntc_mthd_code), flush=True)
            print("{0}".format(e), flush=True)
        count = count + 1


def main():
    global cnnc_mthd_code
    if len(sys.argv) < 3:
        print("사용법: batchresult {배치기준시간} {연계방식코드}", flush=True)
        sys.exit(1)
    batch_date = sys.argv[1]
    cnnc_mthd_code = sys.argv[2]
    batch_result(batch_date, cnnc_mthd_code)
    sys.exit(0)


if __name__ == "__main__":
    main()
