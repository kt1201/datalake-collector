# -*- coding: utf-8 -*-

import copy
import json
import sys

import jdbcconf

classes = None
datatypes = None


def loaddata():
    global classes, datatypes
    if classes is None:
        classes = jdbcconf.load("cfg/classes.json")
    if datatypes is None:
        datatypes = jdbcconf.load("cfg/datatypes.json")


def merge(dict, string):
    dict1 = copy.deepcopy(dict)
    try:
        dict2 = json.loads(string)
    except:
        return dict1
    dict1.update(dict2)
    return dict1


def nvl(dict, name, defvalue=None):
    value = None
    if name in dict:
        value = dict[name]
        if value is not None:
            try:
                length = len(value)
            except:
                length = -1
            if length < 0:
                value = None
    if value is None:
        value = defvalue
    return value


def nvlint(dict, name, defvalue=None):
    value = None
    if name in dict:
        try:
            value = int(dict[name])
        except:
            value = None
    if value is None:
        value = defvalue
    return value


def dbtycode(connection):
    loaddata()
    dbtype = connection["DB_TY_NM"]
    if isinstance(classes[dbtype], str):
        dbtype = classes[dbtype]
    return classes[dbtype]["code"]


def classpath(connection):
    loaddata()
    dbtype = connection["DB_TY_NM"]
    if isinstance(classes[dbtype], str):
        dbtype = classes[dbtype]
    return classes[dbtype]["classpath"]


def classname(connection):
    loaddata()
    dbtype = connection["DB_TY_NM"]
    if isinstance(classes[dbtype], str):
        dbtype = classes[dbtype]
    return classes[dbtype]["classname"]


def url(connection):
    loaddata()
    dbtype = connection["DB_TY_NM"]
    if isinstance(classes[dbtype], str):
        dbtype = classes[dbtype]
    return classes[dbtype]["url"].format(**connection)


def sample(connection):
    loaddata()
    dbtype = connection["DB_TY_NM"]
    if isinstance(classes[dbtype], str):
        dbtype = classes[dbtype]
    return classes[dbtype]["sample"]


def hivetablename(connection, table):
    tablename = []
    if connection is not None:
        if len(connection["HIVE_DB_NM"]) > 0:
            tablename.append(connection["HIVE_DB_NM"])
    if table is not None:
        haveHiveTablename = False
        if len(table["HIVE_TABLE_NM"]) > 0:
            haveHiveTablename = True
            tablename.append(table["HIVE_TABLE_NM"])
        if not haveHiveTablename:
            if "TABLE_ENG_NM" in table:
                if len(table["TABLE_ENG_NM"]) > 0:
                    tablename.append(table["TABLE_ENG_NM"])
    return ".".join(tablename)


def hivecolumnname(column):
    loaddata()
    if len(column["HIVE_COL_NM"]) > 0:
        return column["HIVE_COL_NM"]
    return column["TABLE_ATRB_ENG_NM"]


def hivedatatype(column):
    loaddata()
    datatype = {"NAME": column["HIVE_ATRB_TY_NM"].upper(), "LT_VALUE": None,
                "LENGTH": False, "ANONYM": "NULL"}
    if len(column["HIVE_ATRB_TY_NM"]) == 0:
        datatype["NAME"] = column["TABLE_ATRB_TY_NM"].upper()
    if datatype["NAME"] in datatypes:
        if isinstance(datatypes[datatype["NAME"]], str):
            datatype["NAME"] = datatypes[datatype["NAME"]]
            datatype.update(datatypes[datatype["NAME"]])
        else:
            datatype.update(datatypes[datatype["NAME"]])
    else:
        print("데이터형을 찾을 수 없음: {0}".format(datatype["NAME"]), flush=True)
        datatype["NAME"] = datatypes[""]
        datatype.update(datatypes[datatypes[""]])
    return datatype


def hivecolumndef(column):
    comment = ""
    datatype = hivedatatype(column)
    columndef = "{0} {1}".format(hivecolumnname(column), datatype["NAME"])
    if datatype["LENGTH"] and len(column["TABLE_ATRB_LT_VALUE"]) > 0:
        columndef += "({0})".format(column["TABLE_ATRB_LT_VALUE"])
    if column["TABLE_ATRB_NULL_POSBL_AT"] == "N":
        if len(comment) > 0:
            comment += " "
        comment += "NOT NULL"
    if len(comment) > 0:
        columndef += " COMMENT '{0}'".format(comment)
    return columndef


def hivetableconstraint(dbtype, table, columns):
    tableconstraint = None
    primarykey = ""
    for column in columns:
        if column["TABLE_ATRB_PK_AT"] == "Y":
            if len(primarykey) == 0:
                primarykey += "PRIMARY KEY ("
            else:
                primarykey += ","
            primarykey += hivecolumnname(column)
    if len(primarykey) > 0:
        primarykey += ")"
    if len(primarykey) > 0:
        tableconstraint = "COMMENT '"
        if len(primarykey) > 0:
            tableconstraint += primarykey
        tableconstraint += "'"
    return tableconstraint


def hiverowformat():
    rowformat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' ESCAPED BY '\\\\'"
    return rowformat


def hivestoredas():
    storedas = "STORED AS SEQUENCEFILE"
    return storedas


def jdbccolumndef1(column):
    if column["COLUMN_SIZE"] is not None:
        if column["COLUMN_SIZE"] > 0:
            return "{0}".format(column["COLUMN_SIZE"])
    return ""


def jdbccolumndef2(column):
    if column["COLUMN_SIZE"] is not None and column["DECIMAL_DIGITS"] is not None:
        if column["COLUMN_SIZE"] > 0 and column["COLUMN_SIZE"] <= 38:
            return "{0},{1}".format(column["COLUMN_SIZE"], column["DECIMAL_DIGITS"])
    if column["COLUMN_SIZE"] is not None:
        if column["COLUMN_SIZE"] > 0 and column["COLUMN_SIZE"] <= 38:
            return "{0}".format(column["COLUMN_SIZE"])
    return ""


def jdbcltvalue(column):
    loaddata()
    datatype = {"NAME": column["TYPE_NAME"].upper(), "LT_VALUE": None,
                "LENGTH": False, "ANONYM": "NULL"}
    if datatype["NAME"] in datatypes:
        if isinstance(datatypes[datatype["NAME"]], str):
            datatype.update(datatypes[datatypes[datatype["NAME"]]])
        else:
            datatype.update(datatypes[datatype["NAME"]])
    else:
        datatype.update(datatypes[datatypes[""]])
    if datatype["LT_VALUE"] is not None:
        return eval(datatype["LT_VALUE"])
    return ""


def jdbctablename(connection, table):
    tablename = []
    if connection is not None:
        if "DB_ACNT_NM" in connection:
            if len(connection["DB_ACNT_NM"]) > 0:
                tablename.append(connection["DB_ACNT_NM"])
    if table is not None:
        if "TABLE_ENG_NM" in table:
            if len(table["TABLE_ENG_NM"]) > 0:
                tablename.append(table["TABLE_ENG_NM"])
    return ".".join(tablename)


def jdbcexpression(column, kwds):
    expression = column["TABLE_ATRB_ENG_NM"]
    if len(column["TABLE_ATRB_EXPR"]) > 0:
        expression = column["TABLE_ATRB_EXPR"]
    return expression.format(**kwds)


def jdbctarget(column):
    expression = column["TABLE_ATRB_ENG_NM"]
    if column["DSTNG_TRGET_AT"] == 'Y' or column["DSTNG_TRGET_AT"] == 'E':
        datatype = hivedatatype(column)
        expression = datatype["ANONYM"]
    elif len(column["TABLE_ATRB_EXPR"]) > 0:
        expression = column["TABLE_ATRB_EXPR"]
    return "{0} AS {1}".format(expression, hivecolumnname(column))


def jdbcselect(conf, connection, table, with_condition=True):
    targets = []
    condition = ""
    if len(table["WHERE_INFO_NM"]) > 0:
        condition += "( {0} )".format(table["WHERE_INFO_NM"])
    if len(table["APD_WHERE"]) > 0:
        if len(condition) > 0:
            condition += " AND "
        condition += "( {0} )".format(table["APD_WHERE"])
    if len(condition) == 0:
        condition = "( 1 = 1 )"
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is not None or with_condition:
            targets.append(jdbctarget(column))
    if with_condition:
        condition += " AND ( $CONDITIONS )"
    query = "SELECT {0} FROM {1} WHERE {2}".format(
        ", ".join(targets), jdbctablename(connection, table), condition)
    if conf["sample"] is not None:
        query = sample(connection).format(query, conf["sample"])
    return query


def jdbcselect2(conf, connection, table, with_condition=True):
    targets = []
    condition = ""
    if len(table["WHERE_INFO_NM"]) > 0:
        condition += "( {0} )".format(table["WHERE_INFO_NM"])
    if len(table["APD_WHERE"]) > 0:
        if len(condition) > 0:
            condition += " AND "
        condition += "( {0} )".format(table["APD_WHERE"])
    if len(condition) == 0:
        condition = "( 1 = 1 )"
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is not None or with_condition:
            targets.append(hivecolumnname(column))
    if with_condition:
        condition += " AND ( $CONDITIONS )"
    query = "SELECT {0} FROM {1} WHERE {2}".format(
        ", ".join(targets), hivetablename(connection, table), condition)
    if conf["sample"] is not None:
        query = sample(connection).format(query, conf["sample"])
    return query


def jdbcinsert(connection, table):
    columns = []
    values = []
    for column in table["COLUMNS"]:
        if column["DB_TABLE_ATRB_SN"] is not None:
            columns.append(column["TABLE_ATRB_ENG_NM"])
            values.append("?")
    return "INSERT INTO {0} ( {1} ) VALUES ( {2} )".format(jdbctablename(connection, table), ", ".join(columns), ", ".join(values))


def jdbcdelete(connection, table):
    return "DELETE FROM {0}".format(jdbctablename(connection, table))


def location(conf, jdbc, partition, connection, table, with_partition=True):
    kwds = {"DBTYPE": connection["DB_TY_NM"], "BATCH_DATE": partition,
            "APD_CHK_COL": table["APD_CHK_COL"]}
    location = None
    stderr = None
    partition_spec = []
    if with_partition:
        for column in table["COLUMNS"]:
            if column["DB_TABLE_ATRB_SN"] is None:
                partition_spec.append(
                    column["HIVE_COL_NM"] + " = " + jdbcexpression(column, kwds))
    query = "DESCRIBE FORMATTED {0}".format(
        hivetablename(connection, table))
    if len(partition_spec) > 0:
        query += " PARTITION ( {0} )".format(", ".join(partition_spec))
    try:
        records = jdbc.execute_query(query)
        for record in records:
            if record["COL_NAME"].startswith("Location:"):
                if location is not None:
                    stderr = "파일 위치 정보 조회 중복: {0}: {1}".format(
                        location, record["DATA_TYPE"])
                    return [None, stderr]
                location = record["DATA_TYPE"]
                if conf["namenode"] is not None:
                    if "startswith" in conf["namenode"]:
                        if location.startswith(conf["namenode"]["startswith"]):
                            location = location[len(
                                conf["namenode"]["startswith"]):]
                            if "newvalue" in conf["namenode"]:
                                location = conf["namenode"]["newvalue"] + location
    except Exception as e:
        stderr = str(e)
    return [location, stderr]


def get_partitions(hive, connection, table):
    partitions = {}
    query = "SHOW PARTITIONS {0}.{1}".format(
        connection["HIVE_DB_NM"], table["HIVE_TABLE_NM"])
    try:
        records = hive.execute_query(query)
        for record in records:
            partitions[record["PARTITION"].split("=")[1]] = None
    except Exception as e:
        print("파티션 오류: {0}: {1}".format(
            connection["CNNC_MANAGE_NO"], e), file=sys.stderr, flush=True)
    return partitions
