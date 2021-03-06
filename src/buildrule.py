#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys

import csvutil
import jdbcconf
import jdbcutil


def jdbctablename(table, connection):
    result = ""
    catalog = table.get("TABLE_CAT", None)
    schema = table.get("TABLE_SCHEM", None)
    name = table.get("TABLE_NAME", None)
    if connection["DB_ACNT_NM"] is not None:
        if len(connection["DB_ACNT_NM"]) > 0:
            return name
    if catalog is not None:
        result += "{0}.".format(catalog)
    if schema is not None:
        result += "{0}.".format(schema)
    if name is not None:
        result += name
    return result


def hivetablename(table, connection):
    result = ""
    catalog = table.get("TABLE_CAT", None)
    schema = table.get("TABLE_SCHEM", None)
    name = table.get("TABLE_NAME", None)
    if connection["DB_ACNT_NM"] is not None:
        if len(connection["DB_ACNT_NM"]) > 0:
            if name.find("$") >= 0:
                return name.replace("$", "_")
            return None
    if catalog is not None:
        result += "{0}_".format(catalog.replace("$", "_"))
    if schema is not None:
        result += "{0}_".format(schema.replace("$", "_"))
    if name is not None:
        result += name.replace("$", "_")
    return result


def nullable(column):
    if isinstance(column["NULLABLE"], int):
        if column["NULLABLE"] == 0:
            return "N"
    return "Y"


def primarykey(column, primarykeys):
    if column["TABLE_CAT"] in primarykeys:
        primarykeys = primarykeys[column["TABLE_CAT"]]
        if column["TABLE_SCHEM"] in primarykeys:
            primarykeys = primarykeys[column["TABLE_SCHEM"]]
            if column["TABLE_NAME"] in primarykeys:
                primarykeys = primarykeys[column["TABLE_NAME"]]
                if column["COLUMN_NAME"] in primarykeys:
                    return "Y"
    return "N"


def buildmeta1(meta, connection, dbmskeys):
    record = [None for _ in range(len(meta[0]))]
    dbms = dbmskeys.get(connection["CNNC_MANAGE_NO"], {})
    for index in range(len(meta[0])):
        record[index] = connection.get(
            meta[0][index], dbms.get(meta[0][index], None))
    meta.append(record)


def buildmeta2(meta, connection, tables, dbmskeys, denykeys, amendkeys):
    records = []
    for table in tables:
        name = jdbctablename(table, connection)
        record = [None for _ in range(len(meta[0]))]
        table = {"CNNC_MANAGE_NO": connection["CNNC_MANAGE_NO"], "TABLE_ENG_NM": name, "TABLE_KOREAN_NM": table["REMARKS"], "PART_COLS": "PART_BATCHDATE",
                 "HIVE_TABLE_NM": hivetablename(table, connection), "GTHNLDN_MTH_CODE": "003", "GTHNLDN_MTH_NM": "DropPartition/CreatePartition/Insert"}
        dbms = dbmskeys.get(name, {})
        if "CNNC_MANAGE_NO" in dbms:
            del dbms["CNNC_MANAGE_NO"]
        if "TABLE_ENG_NM" in dbms:
            del dbms["TABLE_ENG_NM"]
        if "TABLE_KOREAN_NM" in dbms:
            if table["TABLE_KOREAN_NM"] is not None:
                if len(table["TABLE_KOREAN_NM"]) > 0:
                    del dbms["TABLE_KOREAN_NM"]
        name = None
        for index in range(len(meta[0])):
            record[index] = dbms.get(
                meta[0][index], table.get(meta[0][index], None))
            if meta[0][index] == "TABLE_ENG_NM":
                name = record[index]
        if name in amendkeys:
            amend = amendkeys[name]
            for index in range(len(meta[0])):
                if meta[0][index] in amend:
                    if amend[meta[0][index]] is not None:
                        record[index] = amend[meta[0][index]]
        if name not in denykeys:
            records.append(record)
    meta.extend(sorted(records))


def buildmeta3(meta, connection, columns, dbmskeys, primarykeys, denykeys2, denykeys3, amendkeys):
    records = []
    for column in columns:
        name = jdbctablename(column, connection)
        record = [None for _ in range(len(meta[0]))]
        if column["TYPE_NAME"].find("(") > 0 and column["TYPE_NAME"].endswith(")"):
            values = column["TYPE_NAME"][:len(
                column["TYPE_NAME"])-1].split("(")
            typename = values[0]
            values = values[1].split(",")
            try:
                if len(values) >= 1:
                    column["COLUMN_SIZE"] = int(values[0])
                if len(values) >= 2:
                    column["DECIMAL_DIGITS"] = int(values[1])
                column["TYPE_NAME"] = typename
            except:
                column["COLUMN_SIZE"] = None
                column["DECIMAL_DIGITS"] = None
        column = {"CNNC_MANAGE_NO": connection["CNNC_MANAGE_NO"], "TABLE_ENG_NM": name, "DB_TABLE_ATRB_SN": column["ORDINAL_POSITION"], "TABLE_ATRB_ENG_NM": column["COLUMN_NAME"], "TABLE_KOREAN_ATRB_NM": column["REMARKS"],
                  "TABLE_ATRB_TY_NM": column["TYPE_NAME"], "TABLE_ATRB_LT_VALUE": jdbcutil.jdbcltvalue(column), "TABLE_ATRB_NULL_POSBL_AT": nullable(column), "TABLE_ATRB_PK_AT": primarykey(column, primarykeys)}
        dbms = dbmskeys.get(name, {})
        dbms = dbms.get(column["TABLE_ATRB_ENG_NM"], {})
        if "CNNC_MANAGE_NO" in dbms:
            del dbms["CNNC_MANAGE_NO"]
        if "TABLE_ENG_NM" in dbms:
            del dbms["TABLE_ENG_NM"]
        if "DB_TABLE_ATRB_SN" in dbms:
            del dbms["DB_TABLE_ATRB_SN"]
        if "TABLE_KOREAN_ATRB_NM" in dbms:
            if column["TABLE_KOREAN_ATRB_NM"] is not None:
                if len(column["TABLE_KOREAN_ATRB_NM"]) > 0:
                    del dbms["TABLE_KOREAN_ATRB_NM"]
        if "TABLE_ATRB_TY_NM" in dbms:
            del dbms["TABLE_ATRB_TY_NM"]
        if "TABLE_ATRB_LT_VALUE" in dbms:
            del dbms["TABLE_ATRB_LT_VALUE"]
        if "TABLE_ATRB_NULL_POSBL_AT" in dbms:
            del dbms["TABLE_ATRB_NULL_POSBL_AT"]
        if "TABLE_ATRB_PK_AT" in dbms:
            del dbms["TABLE_ATRB_PK_AT"]
        tablename = None
        columnname = None
        for index in range(len(meta[0])):
            record[index] = dbms.get(
                meta[0][index], column.get(meta[0][index], None))
            if meta[0][index] == "TABLE_ENG_NM":
                tablename = record[index]
            if meta[0][index] == "TABLE_ATRB_ENG_NM":
                columnname = record[index]
        if tablename in amendkeys:
            if columnname in amendkeys[tablename]:
                amend = amendkeys[tablename][columnname]
                for index in range(len(meta[0])):
                    if meta[0][index] in amend:
                        if amend[meta[0][index]] is not None:
                            record[index] = amend[meta[0][index]]
        if tablename not in denykeys2:
            if tablename in denykeys3:
                if columnname not in denykeys3[tablename]:
                    records.append(record)
            else:
                records.append(record)
    meta.extend(sorted(records))


def run(connection):
    try:
        tables = csvutil.file2dict(
            "cfg/IM-{0}/META-{0}-TABLES.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        tables = csvutil.csv2dict([["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                                    "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"]])
    try:
        columns = csvutil.file2dict(
            "cfg/IM-{0}/META-{0}-COLUMNS.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        columns = csvutil.csv2dict([["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                                     "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"]])
    try:
        primarykeys = csvutil.file2dict(
            "cfg/IM-{0}/META-{0}-PRIMARYKEYS.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        primarykeys = csvutil.csv2dict(
            [["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"]])
    index = 1
    while index < len(columns):
        if columns[index-1] == columns[index]:
            del columns[index]
        else:
            index += 1
    primarykeys = csvutil.dict2keys(
        primarykeys, ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"])
    dbms1 = []
    try:
        dbms1 = csvutil.readfile(
            "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0001.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        dbms1 = [["CNNC_MANAGE_NO", "SYS_NM", "CNTC_MTHD_CODE", "CNTC_MTHD_NM", "DB_TY_CODE", "DB_TY_NM", "DB_ACNT_NM", "HIVE_DB_NM", "DB_SERVICE_NM", "DB_1_SERVER_IP", "DB_1_SERVER_PORT_NO", "DB_2_SERVER_IP", "DB_2_SERVER_PORT_NO", "DB_USER_ID", "DB_USER_SECRET_NO", "REMOTE_SERVER_IP",
                  "REMOTE_SERVER_PORT_NO", "REMOTE_SERVER_USER_ID", "REMOTE_SERVER_USER_SECRET_NO", "REMOTE_DRCTRY_NM", "DATA_TY_CODE", "API_DATA_AUTHKEY_NM", "API_DATA_URL", "NTWK_SE_CODE", "NTWK_SE_NM", "APPLC_SE_CODE", "APPLC_SE_NM", "REGIST_DE", "REGIST_EMPL_NO", "UPDT_DE", "UPDT_EMPL_NO"]]
    dbms2 = []
    try:
        dbms2 = csvutil.readfile(
            "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0002.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        dbms2 = [["CNNC_MANAGE_NO", "TABLE_ENG_NM", "TABLE_KOREAN_NM", "TABLE_DC", "BR_DC", "HASHTAG_CN", "MNGR_NM", "PART_COLS", "WHERE_INFO_NM", "INS_NUM_MAPPERS", "INS_SPLIT_BY_COL",
                  "APD_WHERE", "APD_CHK_COL", "HIVE_TABLE_NM", "BIGDATA_GTRN_AT", "SCHDUL_APPLC_AT", "GTHNLDN_MTH_CODE", "GTHNLDN_MTH_NM", "REGIST_DE", "REGIST_EMPL_NO", "UPDT_DE", "UPDT_EMPL_NO"]]
    dbmskeys2 = csvutil.dict2keys(csvutil.csv2dict(dbms2), ["TABLE_ENG_NM"])
    dbms3 = []
    try:
        dbms3 = csvutil.readfile(
            "cfg/IM-{0}/DBMS-{0}-T_RULE_META_0003.csv".format(connection["CNNC_MANAGE_NO"]))
    except:
        dbms3 = [["CNNC_MANAGE_NO", "TABLE_ENG_NM", "DB_TABLE_ATRB_SN", "TABLE_ATRB_ENG_NM", "TABLE_KOREAN_ATRB_NM", "TABLE_ATRB_EXPR", "TABLE_ATRB_DC", "DSTNG_TRGET_AT", "TABLE_ATRB_TY_NM",
                  "TABLE_ATRB_LT_VALUE", "HIVE_COL_NM", "HIVE_ATRB_TY_NM", "TABLE_ATRB_NULL_POSBL_AT", "TABLE_ATRB_PK_AT", "REGIST_DE", "REGIST_EMPL_NO", "UPDT_DE", "UPDT_EMPL_NO"]]
    dbmskeys3 = csvutil.dict2keys(csvutil.csv2dict(
        dbms3), ["TABLE_ENG_NM", "TABLE_ATRB_ENG_NM"])
    denykeys2 = None
    try:
        deny2 = csvutil.readfile(
            "cfg/IM-{0}/DENY-{0}-T_RULE_META_0002.csv".format(connection["CNNC_MANAGE_NO"]))
        denykeys2 = csvutil.dict2keys(
            csvutil.csv2dict(deny2), ["TABLE_ENG_NM"])
    except Exception as e:
        denykeys2 = {}
    denykeys3 = None
    try:
        deny3 = csvutil.readfile(
            "cfg/IM-{0}/DENY-{0}-T_RULE_META_0003.csv".format(connection["CNNC_MANAGE_NO"]))
        denykeys3 = csvutil.dict2keys(
            csvutil.csv2dict(deny3), ["TABLE_ENG_NM", "TABLE_ATRB_ENG_NM"])
    except Exception as e:
        denykeys3 = {}
    amendkeys2 = None
    try:
        amend2 = csvutil.readfile(
            "cfg/IM-{0}/AMEND-{0}-T_RULE_META_0002.csv".format(connection["CNNC_MANAGE_NO"]))
        amendkeys2 = csvutil.dict2keys(
            csvutil.csv2dict(amend2), ["TABLE_ENG_NM"])
    except Exception as e:
        amendkeys2 = {}
    amendkeys3 = None
    try:
        amend3 = csvutil.readfile(
            "cfg/IM-{0}/AMEND-{0}-T_RULE_META_0003.csv".format(connection["CNNC_MANAGE_NO"]))
        amendkeys3 = csvutil.dict2keys(
            csvutil.csv2dict(amend3), ["TABLE_ENG_NM", "TABLE_ATRB_ENG_NM"])
    except Exception as e:
        amendkeys3 = {}
    meta1 = [dbms1[0]]
    buildmeta1(meta1, connection, csvutil.dict2keys(
        csvutil.csv2dict(dbms1), ["CNNC_MANAGE_NO"]))
    meta2 = [dbms2[0]]
    buildmeta2(meta2, connection, tables, dbmskeys2, denykeys2, amendkeys2)
    meta3 = [dbms3[0]]
    buildmeta3(meta3, connection, columns, dbmskeys3,
               primarykeys, denykeys2, denykeys3, amendkeys3)
    csvutil.writefile(
        "cfg/IM-{0}/RULE-{0}-T_RULE_META_0001.csv".format(connection["CNNC_MANAGE_NO"]), meta1)
    csvutil.writefile(
        "cfg/IM-{0}/RULE-{0}-T_RULE_META_0002.csv".format(connection["CNNC_MANAGE_NO"]), meta2)
    csvutil.writefile(
        "cfg/IM-{0}/RULE-{0}-T_RULE_META_0003.csv".format(connection["CNNC_MANAGE_NO"]), meta3)


def buildrule(argv):
    jdbcconf.home()
    conf = jdbcconf.conf()
    for arg in argv:
        connection = {}
        try:
            connection = jdbcconf.connection(arg)
        except:
            connection = {"CNNC_MANAGE_NO": arg}
        try:
            run(connection)
            print("Rule 생성 성공: {0}".format(arg), flush=True)
        except Exception as e:
            print("Rule 생성 실패: {0}: {1}".format(arg, e), flush=True)


def main():
    if len(sys.argv) < 2:
        print("사용법: buildrule {연결관리번호}...", flush=True)
        sys.exit(1)
    buildrule(sys.argv[1:])


if __name__ == "__main__":
    main()
