#!/usr/bin/python3
# -*- coding: utf-8 -*-

import copy
import json
import os
import requests
import subprocess
import sys

import jdbc2json
import jdbcconf

conf = {}
exitcode = 0


def initalize():
    global conf
    jdbcconf.home()
    conf = jdbcconf.load("cfg/healthcheck.json")


def healthcheck(conf):
    global exitcode
    checked = {}
    for key in conf.keys():
        if key == "repository":
            if isinstance(conf[key], list):
                for item in conf[key]:
                    if checkrepository(conf, item) and key not in checked:
                        print("설정 항목 복제: {0}".format({key: item}), flush=True)
                        checked[key] = item
            elif isinstance(conf[key], dict):
                if checkrepository(conf, conf[key]):
                    print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
                    checked[key] = conf[key]
            else:
                exitcode = 1
                print("JDBC 설정이 유효하지 않음: {0}".format(conf[key]), flush=True)
            if key not in checked:
                exitcode = 1
                print("유효한 JDBC 연결이 없음", flush=True)
        elif key == "namenode":
            if isinstance(conf[key], dict):
                checked[key] = {}
                for key2 in conf[key].keys():
                    if key2 == "newvalue":
                        if isinstance(conf[key][key2], list):
                            for item in conf[key][key2]:
                                if checknamenode(conf, item) and key2 not in checked[key]:
                                    print("설정 항목 복제: {0}".format(
                                        {key: {key2: item}}), flush=True)
                                    checked[key][key2] = item
                        elif isinstance(conf[key][key2], str):
                            if checknamenode(conf, conf[key][key2]):
                                print("설정 항목 복제: {0}".format(
                                    {key: {key2: conf[key][key2]}}), flush=True)
                                checked[key][key2] = conf[key][key2]
                        else:
                            exitcode = 1
                            print("네임노드 설정이 유효하지 않음: {0}".format(
                                conf[key]), flush=True)
                        if key2 not in checked[key]:
                            exitcode = 1
                            print("유효한 네임노드가 없음", flush=True)
                    else:
                        print("설정 항목 복제: {0}".format(

                            {key: {key2: conf[key][key2]}}), flush=True)
                        checked[key][key2] = conf[key][key2]
            elif conf[key] is None:
                print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
                checked[key] = conf[key]
            else:
                exitcode = 1
                print("네임노드 설정이 유효하지 않음: {0}".format(conf[key]), flush=True)
        elif key == "hive":
            if isinstance(conf[key], list):
                for item in conf[key]:
                    if checkhive(conf, item) and key not in checked:
                        print("설정 항목 복제: {0}".format({key: item}), flush=True)
                        checked[key] = item
            elif isinstance(conf[key], dict):
                if checkhive(conf, conf[key]):
                    print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
                    checked[key] = conf[key]
            else:
                exitcode = 1
                print("HIVE 설정이 유효하지 않음: {0}".format(conf[key]), flush=True)
            if key not in checked:
                exitcode = 1
                print("유효한 HIVE 연결이 없음", flush=True)
        elif key == "proxy":
            if isinstance(conf[key], list):
                for item in conf[key]:
                    if checkproxy(conf, item) and key not in checked:
                        print("설정 항목 복제: {0}".format({key: item}), flush=True)
                        checked[key] = item
            elif isinstance(conf[key], str):
                if checkproxy(conf, conf[key]):
                    print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
                    checked[key] = conf[key]
            elif conf[key] is None:
                print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
                checked[key] = conf[key]
            else:
                exitcode = 1
                print("PROXY 설정이 유효하지 않음: {0}".format(conf[key]), flush=True)
            if key not in checked:
                exitcode = 1
                print("유효한 PROXY 연결이 없음", flush=True)
        else:
            print("설정 항목 복제: {0}".format({key: conf[key]}), flush=True)
            checked[key] = conf[key]
    return checked


def checkrepository(conf, repository):
    query = "SELECT 'off' TRANSACTION_READ_ONLY"
    if repository["classname"] == "org.postgresql.Driver":
        query = "SHOW TRANSACTION_READ_ONLY"
    if repository["classname"] == "oracle.jdbc.OracleDriver" or repository["classname"] == "com.tmax.tibero.jdbc.TbDriver":
        query = "SELECT 'off' TRANSACTION_READ_ONLY FROM DUAL"
    if repository["username"] is not None:
        if repository["username"].startswith("$"):
            if repository["username"][1:] in os.environ:
                repository["username"] = os.environ[repository["username"][1:]]
    result = jdbc2json.command(repository, query)
    if result is not None:
        if isinstance(result, list):
            if len(result) == 1:
                if "TRANSACTION_READ_ONLY" in result[0]:
                    if result[0]["TRANSACTION_READ_ONLY"] == "off":
                        print("JDBC 연결 성공: {0}".format(repository), flush=True)
                        return True
                    else:
                        print("JDBC 대기 상태: {0}".format(repository), flush=True)
                        return False
    print("JDBC 연결 실패: {0}: {1}".format(repository, result), flush=True)
    return False


def checknamenode(conf, namenode):
    command = []
    command.append("hdfs")
    command.append("dfs")
    command.append("-ls")
    command.append(namenode)
    proc = subprocess.Popen(command, stdin=subprocess.PIPE)
    try:
        out = proc.communicate(timeout=60)
        if proc.returncode == 0:
            return True
        print("네임노드 연결 실패: {0}".format(namenode), flush=True)
    except Exception as e:
        proc.kill()
        print("Exception: {0}: {1}".format(namenode, e), flush=True)
    return False


def checkhive(conf, hive):
    query = "SELECT 'X' DUMMY"
    hive = copy.deepcopy(hive)
    hive["url"] = hive["url"].format(HIVE_DB_NM="")
    result = jdbc2json.command(hive, query)
    if result is not None:
        if isinstance(result, list):
            if len(result) == 1:
                if "DUMMY" in result[0]:
                    if result[0]["DUMMY"] == "X":
                        print("HIVE 연결 성공: {0}".format(hive), flush=True)
                        return True
                    else:
                        print("HIVE 연결 실패: {0}: {1}".format(
                            hive, result), flush=True)
                        return False
    print("HIVE 연결 실패: {0}: {1}".format(hive, result), flush=True)
    return False


def checkproxy(conf, proxy):
    count = 0
    for index in range(6):
        try:
            req = requests.get(proxy, timeout=10)
            print("PROXY 연결 성공: {0}: {1}".format(
                proxy, req.reason, req.text), flush=True)
            print(req.text, flush=True)
            return True
        except:
            count = count + 1
    print("PROXY 연결 실패: {0}".format(proxy), flush=True)
    return False


def main():
    global exitcode
    initalize()
    checked = healthcheck(conf)
    value = json.dumps(checked, indent=4, ensure_ascii=False)
    if exitcode == 0:
        filename = "cfg/datalake.json"
        try:
            file = open(filename, "w", encoding="utf-8")
            file.write(value)
            file.close()
            print("설정 구성 성공", flush=True)
        except:
            exitcode = 1
            print("파일에 쓸 수 없음: " + filename, flush=True)
    if exitcode != 0:
        print("설정 구성 실패", flush=True)
    print(value)
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
