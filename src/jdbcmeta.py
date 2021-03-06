#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys

import csvutil
import jdbc2json
import jdbcconf
import json


def getschemas(jdbc):
    result = []
    catalogs = jdbc.catalogs()
    if len(catalogs) == 0:
        catalogs.append(None)
    for catalog in catalogs:
        schemas = jdbc.schemas(catalog)
        for schema in schemas:
            result.append([schema["TABLE_CATALOG"], schema["TABLE_SCHEM"]])
    if len(result) == 0:
        result.append([None, None])
    return result


def getheaders(header):
    headers = [None, None, None]
    for index in range(len(header)):
        if header[index] == "TABLE_CAT":
            headers[0] = index
        if header[index] == "TABLE_SCHEM":
            headers[1] = index
        if header[index] == "TABLE_NAME":
            headers[2] = index
    return headers


def run(jdbc, connection):
    database = {}
    schemas = [None, None]
    tables = []
    columns = []
    primarykeys = []
    indices = []
    exportedkeys = []
    importedkeys = []
    header = [True, True, True, True, True, True]
    try:
        database = jdbc.database()
    except Exception as e:
        print("Database: {0}".format(e), flush=True)
    try:
        schemas = getschemas(jdbc)
        print("Schemas: {0}".format(schemas), flush=True)
    except Exception as e:
        print("Schemas: {0}".format(e), flush=True)
    if connection["DB_ACNT_NM"] is not None:
        if len(connection["DB_ACNT_NM"]) > 0:
            schemas = [[None, connection["DB_ACNT_NM"]]]
            print("Schemas: {0}".format(schemas), flush=True)
    jdbccount = 1
    for schema in schemas:
        try:
            records = jdbc.tables(schema[0], schema[1], types=[
                                  "TABLE", "VIEW"], csv=True, header=header[0])
            if header[0] == True:
                tables.append(records.pop(0))
                header[0] = False
            tables.extend(records)
        except Exception as e:
            print("Tables: {0}".format(e), flush=True)
        try:
            records = jdbc.columns(
                schema[0], schema[1], csv=True, header=header[1])
            if header[1] == True:
                header[1] = False
            columns.extend(records)
        except Exception as e:
            print("Columns: {0}".format(e), flush=True)
        jdbccount += 2
        if jdbccount >= 100:
            jdbc.disconnect()
            jdbccount = 0
    if len(tables) == 0:
        tables.append(["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                       "TYPE_CAT", "TYPE_SCHEMA", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"])
    if len(columns) == 0:
        columns.append(["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"])
    records = columns[1:]
    columns = [columns[0]]
    tablekeys = csvutil.dict2keys(csvutil.csv2dict(
        tables), ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"])
    headers = getheaders(columns[0])
    for column in records:
        if column[headers[0]] in tablekeys:
            if column[headers[1]] in tablekeys[column[headers[0]]]:
                if column[headers[2]] in tablekeys[column[headers[0]]][column[headers[1]]]:
                    columns.append(column)
    headers = getheaders(tables[0])
    for table in tables[1:]:
        try:
            records = jdbc.indices(
                schema[0], schema[1], table[headers[2]], csv=True, header=header[2])
            if header[2] == True:
                header[2] = False
            indices.extend(records)
        except Exception as e:
            print("Indices: {0}".format(e), flush=True)
        try:
            records = jdbc.primarykeys(
                schema[0], schema[1], table[headers[2]], csv=True, header=header[3])
            if header[3] == True:
                header[3] = False
            primarykeys.extend(records)
        except Exception as e:
            print("PrimaryKeys: {0}".format(e), flush=True)
        try:
            records = jdbc.exportedkeys(
                schema[0], schema[1], table[headers[2]], csv=True, header=header[4])
            if header[4] == True:
                header[4] = False
            exportedkeys.extend(records)
        except Exception as e:
            print("ExportedKeys: {0}".format(e), flush=True)
        try:
            records = jdbc.importedkeys(
                schema[0], schema[1], table[headers[2]], csv=True, header=header[5])
            if header[5] == True:
                header[5] = False
            importedkeys.extend(records)
        except Exception as e:
            print("ImportedKeys: {0}".format(e), flush=True)
        jdbccount += 4
        if jdbccount >= 100:
            jdbc.disconnect()
            jdbccount = 0
    if len(indices) == 0:
        indices.append(["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                        "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"])
    if len(primarykeys) == 0:
        primarykeys.append(["TABLE_CAT", "TABLE_SCHEM",
                            "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"])
    if len(exportedkeys) == 0:
        exportedkeys.append(["PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                             "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"])
    if len(importedkeys) == 0:
        importedkeys.append(["PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                             "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"])
    file = open(
        "cfg/IM-{0}/META-{0}-DATABASE.json".format(connection["CNNC_MANAGE_NO"]), "w", encoding="utf-8")
    file.write(json.dumps(database, indent=4,
                          sort_keys=True, ensure_ascii=False))
    file.close()
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-TABLES.csv".format(connection["CNNC_MANAGE_NO"]), tables)
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-COLUMNS.csv".format(connection["CNNC_MANAGE_NO"]), columns)
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-INDICES.csv".format(connection["CNNC_MANAGE_NO"]), indices)
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-PRIMARYKEYS.csv".format(connection["CNNC_MANAGE_NO"]), primarykeys)
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-EXPORTEDKEYS.csv".format(connection["CNNC_MANAGE_NO"]), exportedkeys)
    csvutil.writefile(
        "cfg/IM-{0}/META-{0}-IMPORTEDKEYS.csv".format(connection["CNNC_MANAGE_NO"]), importedkeys)


def jdbcmeta(argv):
    jdbcconf.home()
    jdbcconf.conf()
    for arg in argv:
        connection = jdbcconf.connection(arg)
        jdbc = jdbc2json.new(connection)
        try:
            run(jdbc, connection)
            print("메타 추출 성공: {0}".format(arg), flush=True)
        except Exception as e:
            print("메타 추출 실패: {0}: {1}".format(arg, e), flush=True)


def main():
    if len(sys.argv) < 2:
        print("사용법: jdbcmeta {연결관리번호}...", flush=True)
        sys.exit(1)
    jdbcmeta(sys.argv[1:])


if __name__ == "__main__":
    main()
