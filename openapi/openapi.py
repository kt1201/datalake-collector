# -*- coding: utf-8 -*-

import json
import os
import sys
import threading
import time
import urllib.request
import xmltodict

try:
    sys.path.insert(0, os.path.join(os.environ["DATALAKE_COLLECTOR"], "src"))
    import csv2seqfile
    import csvutil
    import hiveanalyze
    import hiveclearpartition
    import hivecreatetable
    import hivecreatepartition
    import hivedroppartition
    import hiveverify
    import jdbc2json
    import jdbcconf
    import jdbcutil
except Exception as e:
    print("Exception: IMPORT: {0}".format(e), file=sys.stderr, flush=True)
    sys.exit(1)

conf = None
lock = threading.Lock()
results = {}


def config():
    global conf
    if conf is None:
        jdbcconf.home()
        conf = jdbcconf.conf()
    return conf


def connection(cnnc_manage_no):
    return jdbcconf.connection(cnnc_manage_no)


def table(cnnc_manage_no, table_eng_nm):
    if isinstance(cnnc_manage_no, dict):
        cnnc_manage_no = cnnc_manage_no["CNNC_MANAGE_NO"]
    return jdbcconf.table(cnnc_manage_no, table_eng_nm)


def tables(cnnc_manage_no, argv):
    if isinstance(cnnc_manage_no, str):
        cnnc_manage_no = connection(cnnc_manage_no)
    return jdbcconf.tables(config(), cnnc_manage_no, argv)


def print_results():
    global results
    print("", flush=True)
    print(json.dumps(results, ensure_ascii=False), end="", flush=True)


def haskey(response, filter, name):
    if filter is not None:
        for key in filter:
            if key not in response:
                raise Exception(name + "[\"{0}\"]".format(key))
            haskey(response[key], filter[key], name + "[\"{0}\"]".format(key))


def request(url, filters=None, convert=None, encoding="utf-8"):
    conf = config()
    if conf["proxy"] is not None:
        proxy = urllib.request.ProxyHandler(
            {"http": conf["proxy"], "https": conf["proxy"]})
        opener = urllib.request.build_opener(proxy)
        urllib.request.install_opener(opener)
    for count in range(10):
        res = urllib.request.urlopen(url)
        restext = None
        try:
            restext = res.read().decode(encoding)
        except Exception as e:
            restext = "Read Failed: {0}".format(e)
        try:
            status = res.status
        except:
            status = res.code
        if status == 200:
            try:
                if callable(convert):
                    response = convert(restext)
                else:
                    if restext.startswith("<"):
                        response = xmltodict.parse(restext)
                    else:
                        response = json.loads(restext)
                if isinstance(filters, list):
                    passed = 0
                    key = None
                    for filter in filters:
                        try:
                            haskey(response, filter, "response")
                            passed += 1
                        except Exception as e:
                            key = e
                    if passed == 0 and key is not None:
                        raise key
                else:
                    haskey(response, filters, "response")
                return response
            except Exception as e:
                print("Parse Failed: {0}: {1}".format(url, e),
                      file=sys.stderr, flush=True)
                print(restext, file=sys.stderr, flush=True)
        else:
            print("Request Failed: {0}: {1}".format(
                status, url), file=sys.stderr, flush=True)
            print(restext, file=sys.stderr, flush=True)
        if count < 6:
            time.sleep(10)
        else:
            time.sleep(60)
    print("EXIT: Request Failed", file=sys.stderr, flush=True)
    sys.exit(1)


def get_partitions(hive, cnnc_manage_no, table_eng_nm):
    if isinstance(cnnc_manage_no, str):
        cnnc_manage_no = connection(cnnc_manage_no)
    if isinstance(table_eng_nm, str):
        table_eng_nm = table(cnnc_manage_no, table_eng_nm)
    return jdbcutil.get_partitions(hive, cnnc_manage_no, table_eng_nm)


def csvfilename(batch_date, cnnc_manage_no, table_eng_nm, partition=None):
    if partition is not None:
        filename = "{0}{1}-{2}-{3}-{4}.source.csv".format(
            conf["csv"], batch_date, cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"], partition)
    else:
        filename = "{0}{1}-{2}-{3}.source.csv".format(
            conf["csv"], batch_date, cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"])
    return filename


def hivestore(jdbc, hive, conf, batch_date, cnnc_manage_no, records, table_eng_nm, partition=None):
    global results
    exitcode = 0
    columns = 0
    if isinstance(cnnc_manage_no, str):
        cnnc_manage_no = connection(cnnc_manage_no)
    if isinstance(table_eng_nm, str):
        table_eng_nm = table(cnnc_manage_no, table_eng_nm)
    if records is None:
        records = []
    for column in table_eng_nm["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is not None:
            columns = columns + 1
    if "Records" not in results:
        results["Records"] = 0
    results["Records"] = results["Records"] + len(records)
    filename = csvfilename(batch_date, cnnc_manage_no,
                           table_eng_nm, partition=partition)
    csvutil.writefile(filename, records, columns)
    print("W", end="", file=sys.stdout, flush=True)
    result = hivecreatetable.run(None, hive, conf, batch_date,
                                 cnnc_manage_no, table_eng_nm, partition)
    exitcode = result[0]
    if exitcode != 0:
        if result[1] is not None:
            print(result[1], flush=True)
        if result[2] is not None:
            print(result[2], flush=True)
        return exitcode
    print("T", end="", file=sys.stdout, flush=True)
    result = hivedroppartition.run(
        None, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, partition)
    exitcode = result[0]
    if exitcode != 0:
        if result[1] is not None:
            print(result[1], flush=True)
        if result[2] is not None:
            print(result[2], flush=True)
        return exitcode
    print("D", end="", file=sys.stdout, flush=True)
    result = hivecreatepartition.run(
        None, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, partition)
    exitcode = result[0]
    if exitcode != 0:
        if result[1] is not None:
            print(result[1], flush=True)
        if result[2] is not None:
            print(result[2], flush=True)
        return exitcode
    print("P", end="", file=sys.stdout, flush=True)
    exitcode = csv2seqfile.run(
        hive, conf, batch_date, cnnc_manage_no, filename, table_eng_nm, partition)
    if exitcode != 0:
        print("CSV 파일 로드 실패: {0}".format(filename), flush=True)
        return exitcode
    print("S", end="", file=sys.stdout, flush=True)
    result = hiveanalyze.run(None, hive, conf, batch_date,
                             cnnc_manage_no, table_eng_nm, partition)
    exitcode = result[0]
    if exitcode != 0:
        if result[1] is not None:
            print(result[1], flush=True)
        if result[2] is not None:
            print(result[2], flush=True)
        return exitcode
    else:
        results = jdbcutil.merge(results, result[1])
    print("A", end="", file=sys.stdout, flush=True)
    if partition is None:
        result = hiveclearpartition.run(
            None, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, partition)
        exitcode = result[0]
        if exitcode != 0:
            if result[1] is not None:
                print(result[1], flush=True)
            if result[2] is not None:
                print(result[2], flush=True)
            return exitcode
        else:
            results = jdbcutil.merge(results, result[1])
        print("C", end="", file=sys.stdout, flush=True)
    result = hiveverify.run(None, hive, conf, batch_date,
                            cnnc_manage_no, table_eng_nm, partition)
    exitcode = result[0]
    print("V", end="", file=sys.stdout, flush=True)
    if exitcode != 0:
        if result[1] is not None:
            print(result[1], flush=True)
        if result[2] is not None:
            print(result[2], flush=True)
        return exitcode
    else:
        if partition is not None:
            if "MD5" not in results:
                results["MD5"] = {}
            results["MD5"] = jdbcutil.merge(
                results["MD5"], json.dumps(json.loads(result[1])["MD5"], ensure_ascii=False))
        else:
            results = jdbcutil.merge(results, result[1])
    return exitcode


def main(handlers):

    exitcode = 0

    if len(sys.argv) < 3:
        print("사용법: {0} {{배치기준시간}} {{연결관리번호}} [테이블명] ...".format(
            os.path.basename(sys.argv[0])), flush=True)
        sys.exit(1)

    # 설정 파일 읽기
    conf = config()
    batch_date = sys.argv[1]
    cnnc_manage_no = connection(sys.argv[2])
    table_eng_nm_list = tables(cnnc_manage_no, sys.argv[3:])
    jdbc = jdbc2json.new(cnnc_manage_no)
    hive = jdbc2json.new(conf["hive"], cnnc_manage_no)

    # 1개씩 순차적 수집/적재 수행
    while len(table_eng_nm_list) > 0:
        table_eng_nm = None
        tablename = sorted(table_eng_nm_list.keys()).pop(0)
        table_eng_nm = table_eng_nm_list[tablename]
        del table_eng_nm_list[tablename]
        if cnnc_manage_no["CNNC_MANAGE_NO"] in handlers:
            if table_eng_nm["TABLE_ENG_NM"] in handlers[cnnc_manage_no["CNNC_MANAGE_NO"]]:
                print("수집 시작: {0} - {1}".format(
                    cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                contents = handlers[cnnc_manage_no["CNNC_MANAGE_NO"]][table_eng_nm["TABLE_ENG_NM"]]["handler"](
                    jdbc, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, handlers[cnnc_manage_no["CNNC_MANAGE_NO"]][table_eng_nm["TABLE_ENG_NM"]]["opaque"])
                if contents is not None:
                    print("수집 성공: {0} - {1}".format(
                        cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                    print("적재 시작: {0} - {1}".format(
                        cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                    result = hivestore(
                        jdbc, hive, conf, batch_date, cnnc_manage_no, contents, table_eng_nm)
                    print("", file=sys.stdout, flush=True)
                    if result == 0:
                        print("적재 성공: {0} - {1}".format(
                            cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                    else:
                        print("적재 실패: {0} - {1}".format(
                            cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                        exitcode = result
                else:
                    print("수집/적재 성공: {0} - {1}".format(
                        cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
            else:
                print("핸들러가 없음: {0} - {1}".format(
                    cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
                exitcode = 1
        else:
            print("핸들러가 없음: {0} - {1}".format(
                cnnc_manage_no["CNNC_MANAGE_NO"], table_eng_nm["TABLE_ENG_NM"]), flush=True)
            exitcode = 1
        print_results()
    sys.exit(exitcode)
