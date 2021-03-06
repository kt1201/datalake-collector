#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import datetime
import os

import openapi


def getList(jdbc, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, opaque):

    # 수집 데이터 저장 변수
    contents = []

    filename = os.path.join(
        os.environ["DATALAKE_COLLECTOR"], "doc/dat/sample.csv")
    file = open(filename, "rt", encoding="euc-kr")
    reader = csv.reader(file)

    # 파일 데이터 가공 및 수집
    for record in reader:
        element = []
        for column in table_eng_nm["COLUMNS"]:
            if column["DB_TABLE_ATRB_SN"] is not None:
                value = record.pop(0)
                if isinstance(value, str):
                    value = value.strip()
                if column["HIVE_ATRB_TY_NM"] == "DECIMAL":
                    value = float(value)
                if column["HIVE_ATRB_TY_NM"] == "DATE":
                    value = datetime.datetime.strptime(
                        value, "%Y-%m-%d").date()
                element.append(value)
        contents.append(element)

    return contents


handlers = {
    "template3": {
        "Incheon_West_Restaurant": {"handler": getList, "opaque": ""}
    }
}


if __name__ == "__main__":
    openapi.main(handlers)
