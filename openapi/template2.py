#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import sys
import urllib.parse

import openapi

min_partitions = 2
max_partitions = 10


# 데이터 json 변환 및 오류 판별
def convert(response):
    value = response
    if value.startswith("{err:"):
        if not value.startswith("{err:\"30\""):
            raise Exception("오류 메시지: {0}".format(value))
    else:
        value = json.loads(value)
    return value


def collect_DT_435001N_001(jdbc, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, opaque):

    # OPENAPI 데이터 수집 URL 및 Query Parameter
    baseurl = "{0}".format(cnnc_manage_no["API_DATA_URL"])
    params = {
        "method": "getList",
        "format": "json",
        "jsonVD": "Y",
        "prdSe": "M"
    }
    # 인증키를 추가
    params.update(json.loads(cnnc_manage_no["API_DATA_AUTHKEY_NM"]))
    params.update(opaque)

    # 파티션 목록 가져오기
    partitions = openapi.get_partitions(hive, cnnc_manage_no, table_eng_nm)

    # 배치기준일시에서 연도 추출
    last_year = int(batch_date[0:4])

    # OPENAPI 연도별 수집 루프
    for year in range(last_year - max_partitions, last_year + 1):
        contents = []
        partition = "{0:04d}0000000000".format(year)
        # 파티션 유무에 따른 수집 여부 결정, 최근 데이터는 무조건 수집 혹은 재수집
        if last_year - year >= min_partitions and partition in partitions:
            print("파티션이 있음: {0}".format(partition),
                  file=sys.stderr, flush=True)
            continue
        # 월별 수집 루프
        for month in range(1, 13):
            yyyymm = "{0:04d}{1:02d}".format(year, month)
            params["startPrdDe"] = yyyymm
            params["endPrdDe"] = yyyymm

            # OPENAPI 호출
            url = "{0}?{1}".format(baseurl, urllib.parse.urlencode(
                params, quote_via=urllib.parse.quote))
            response = openapi.request(url, convert=convert)

            # 결과값이 string 형이면 데이터가 없음
            if isinstance(response, str):
                print("데이터가 없음: {0}년 {1}월".format(
                    year, month), file=sys.stderr, flush=True)
                continue

            # 수집 데이터 가공 및 수집
            items = []
            if isinstance(response, list):
                items.extend(response)
            elif isinstance(response, dict):
                items.append(response)
            # 레코드별로 가져오기
            for item in items:
                element = []
                # 컬럼별로 가져오기
                for column in table_eng_nm["COLUMNS"]:
                    if column["DB_TABLE_ATRB_SN"] is not None:
                        if column["TABLE_ATRB_ENG_NM"] in item:
                            value = item[column["TABLE_ATRB_ENG_NM"]]
                            if isinstance(value, str):
                                value = value.strip()
                            # Hive 데이터형이 INTERGER인 컬럼은 int로 변환
                            if column["HIVE_ATRB_TY_NM"] == "INTEGER":
                                value = int(value)
                            element.append(value)
                        else:
                            element.append(None)
                contents.append(element)
            print(".", end="", file=sys.stdout, flush=True)

        # Hive에 연도별 파티션 생성 후 레코드 적재
        if openapi.hivestore(jdbc, hive, conf, batch_date, cnnc_manage_no, contents, table_eng_nm, partition=partition) != 0:
            print("파티션 저장 실패: {0}".format(partition),
                  file=sys.stderr, flush=True)

    print("", file=sys.stdout, flush=True)


handlers = {
    "template2": {
        "DT_435001N_001": {"handler": collect_DT_435001N_001, "opaque": {"startPrdDe": "", "endPrdDe": ""}}
    }
}


if __name__ == "__main__":
    openapi.main(handlers)
