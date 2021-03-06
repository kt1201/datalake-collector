#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import sys
import urllib.parse

import openapi


def getBeachLocationList(jdbc, hive, conf, batch_date, cnnc_manage_no, table_eng_nm, opaque):

    # 수집 데이터 저장 변수
    contents = []

    # OPENAPI 데이터 수집 URL 및 Query Parameter
    baseurl = "{0}/{1}".format(
        cnnc_manage_no["API_DATA_URL"], "getBeachLocationList")
    params = {
        "_type": "json",
        "numOfRows": 5,
        "pageNo": 1
    }
    # 인증키를 추가
    params.update(json.loads(cnnc_manage_no["API_DATA_AUTHKEY_NM"]))
    params.update(opaque)

    # OPENAPI 페이지별 수집 루프
    maxPages = 1
    while params["pageNo"] <= maxPages:
        # OPENAPI 호출
        url = "{0}?{1}".format(baseurl, urllib.parse.urlencode(
            params, quote_via=urllib.parse.quote))
        response = openapi.request(url, {"response": {"body": {
                                   "pageNo": None, "totalCount": None, "items": {"item": None}}}}, json.loads)
        # 최대 페이지 수 계산
        params["pageNo"] = response["response"]["body"]["pageNo"]
        totalCount = response["response"]["body"]["totalCount"]
        maxPages = int(
            (totalCount + (params["numOfRows"] - 1)) / params["numOfRows"])
        # 수집 데이터 가공 및 수집
        items = []
        if isinstance(response["response"]["body"]["items"]["item"], list):
            items.extend(response["response"]["body"]["items"]["item"])
        elif isinstance(response["response"]["body"]["items"]["item"], dict):
            items.append(response["response"]["body"]["items"]["item"])
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
                        element.append(value)
                    else:
                        element.append(None)
            contents.append(element)
        print(".", end="", file=sys.stdout, flush=True)
        params["pageNo"] = params["pageNo"] + 1
    print("", file=sys.stdout, flush=True)

    return contents


handlers = {
    "template1": {
        "getBeachLocationList": {"handler": getBeachLocationList, "opaque": {"pubAsAddress": "신평면 부수리"}}
    }
}


if __name__ == "__main__":
    openapi.main(handlers)
