#!/usr/bin/python3
# -*- coding: utf-8 -*-

import contextlib
import os
import sys

import jdbcconf

conf = {}


def initalize():
    global conf
    jdbcconf.home()
    conf = jdbcconf.conf()


def main():
    if len(sys.argv) < 10:
        print(
            "사용법: createcfg {연결관리번호} {수집유형코드} {DBMS유형} {서버IP} {서버포트} {사용자ID} {비밀번호} {서비스명} {HIVE데이터베이스명} [스키마명]", flush=True)
        sys.exit(1)
    initalize()
    with contextlib.suppress(FileExistsError):
        os.mkdir("cfg/IM-{0}".format(sys.argv[1]))
    connection = {"CNNC_MANAGE_NO": sys.argv[1], "CNTC_MTHD_CODE": sys.argv[2], "DB_TY_NM": sys.argv[3], "DB_1_SERVER_IP": sys.argv[4],
                  "DB_1_SERVER_PORT_NO": sys.argv[5], "DB_USER_ID": sys.argv[6], "DB_USER_SECRET_NO": sys.argv[7], "DB_SERVICE_NM": sys.argv[8], "HIVE_DB_NM": sys.argv[9]}
    if len(sys.argv) >= 11:
        connection["DB_ACNT_NM"] = sys.argv[10]
    if len(connection["DB_USER_ID"]) <= 0:
        connection["DB_USER_ID"] = None
    if len(connection["DB_USER_SECRET_NO"]) <= 0:
        connection["DB_USER_SECRET_NO"] = None
    jdbcconf.make_connection_config(conf, connection)


if __name__ == "__main__":
    main()
