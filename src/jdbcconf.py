# -*- coding: utf-8 -*-

import json
import multiprocessing
import os
import platform
import sys

import jdbcutil


def load(filename):
    filename = os.environ["DATALAKE_COLLECTOR"] + filename
    try:
        file = open(filename, "r", encoding="utf-8")
        string = file.read()
        file.close()
    except:
        print("파일을 읽을 수 없음: " + filename, flush=True)
        sys.exit(1)
    return json.loads(string)


def home(chdir=True):
    home = os.environ.get("DATALAKE_COLLECTOR", os.getcwd())
    if not home.endswith(os.path.sep):
        home += os.path.sep
    os.environ["DATALAKE_COLLECTOR"] = home
    if chdir:
        os.chdir(home)
    return home


def conf():
    conf = load("cfg/datalake.json")
    if "repository" not in conf:
        conf["repository"] = {"classpath": None, "classname": None, "url": None,
                              "username": None, "password": None, "fetchsize": None, "check": None}
    if "namenode" not in conf:
        conf["namenode"] = None
    if "hive" not in conf:
        conf["hive"] = {"classpath": None, "classname": None, "url": None,
                        "username": None, "password": None, "fetchsize": None, "check": None}
    if "proxy" not in conf:
        conf["proxy"] = None
    if "condition" not in conf:
        conf["condition"] = None
    if "cntc_mthd_nm" not in conf:
        conf["condition"] = {}
    if "csv" not in conf:
        conf["csv"] = "tmp"
    if "fetchsize" not in conf:
        conf["fetchsize"] = 1000
    if "sample" not in conf:
        conf["sample"] = None
    if "threads" not in conf:
        conf["threads"] = None
    if not conf["csv"].endswith(os.path.sep):
        conf["csv"] = conf["csv"] + os.path.sep
    if conf["threads"] is None:
        conf["threads"] = multiprocessing.cpu_count()
    return conf


def connection(name):
    return load("cfg/IM-{0}/SYS-{0}.json".format(name))


def table(connection, name):
    if isinstance(connection, dict):
        connection = connection["CNNC_MANAGE_NO"]
    return load("cfg/IM-{0}/TAB-{0}-{1}.json".format(connection, name))


def tables(conf, connection, argv):
    tablenames = []
    condition = conf["condition"]
    if len(argv) > 0:
        for arg in argv:
            if arg in connection["TABLE_NM"]:
                tablenames.append(arg)
            else:
                print("테이블명을 찾을수 없습니다: \"{0}\"".format(arg), flush=True)
                sys.exit(1)
        connection["TABLE_NM"] = tablenames
    else:
        tablenames = connection["TABLE_NM"]
    tables = {}
    for tablename in tablenames:
        entry = table(connection, tablename)
        tables[tablename] = entry
    if len(argv) <= 0 and condition is not None:
        for tablename in tablenames:
            entry = tables[tablename]
            orresult = False
            for items in condition:
                andresult = True
                for item in items.keys():
                    if item in entry:
                        if entry[item] != items[item]:
                            andresult = False
                    else:
                        andresult = False
                orresult = orresult or andresult
            if not orresult:
                del tables[tablename]
    return tables


def javacommand(config, connection=None):
    command = []
    command.append("java")
    command.append("-classpath")
    classpath = os.environ["DATALAKE_COLLECTOR"] + \
        "lib/datalake-collector-1.0.2.jar"
    if config is not None:
        if "classpath" in config:
            if config["classpath"] is not None:
                if len(config["classpath"]) > 0:
                    if platform.system() == "Windows":
                        classpath += ";"
                    else:
                        classpath += ":"
                    classpath += os.environ["DATALAKE_COLLECTOR"] + \
                        config["classpath"]
        if "JDBC_CLASSPATH" in config:
            if config["JDBC_CLASSPATH"] is not None:
                if len(config["JDBC_CLASSPATH"]) > 0:
                    if platform.system() == "Windows":
                        classpath += ";"
                    else:
                        classpath += ":"
                    classpath += os.environ["DATALAKE_COLLECTOR"] + \
                        config["JDBC_CLASSPATH"]
        command.append(classpath)
        command.append("-Dfile.encoding=UTF-8")
        if "classname" in config:
            if config["classname"] is not None:
                if len(config["classname"]) > 0:
                    command.append(
                        "-Djdbc.classname={0}".format(config["classname"]))
        if "JDBC_DRIVER" in config:
            if config["JDBC_DRIVER"] is not None:
                if len(config["JDBC_DRIVER"]) > 0:
                    command.append(
                        "-Djdbc.classname={0}".format(config["JDBC_DRIVER"]))
        if "url" in config:
            if config["url"] is not None:
                if len(config["url"]) > 0:
                    url = config["url"]
                    if connection is not None:
                        url = url.format(**connection)
                    command.append("-Djdbc.url={0}".format(url))
        if "JDBC_URL" in config:
            if config["JDBC_URL"] is not None:
                if len(config["JDBC_URL"]) > 0:
                    command.append("-Djdbc.url={0}".format(config["JDBC_URL"]))
        if "username" in config:
            if config["username"] is not None:
                if len(config["username"]) > 0:
                    command.append(
                        "-Djdbc.username={0}".format(config["username"]))
        if "DB_USER_ID" in config:
            if config["DB_USER_ID"] is not None:
                if len(config["DB_USER_ID"]) > 0:
                    command.append(
                        "-Djdbc.username={0}".format(config["DB_USER_ID"]))
        if "password" in config:
            if config["password"] is not None:
                if len(config["password"]) > 0:
                    command.append(
                        "-Djdbc.password={0}".format(config["password"]))
        if "DB_USER_SECRET_NO" in config:
            if config["DB_USER_SECRET_NO"] is not None:
                if len(config["DB_USER_SECRET_NO"]) > 0:
                    command.append(
                        "-Djdbc.password={0}".format(config["DB_USER_SECRET_NO"]))
        if "fetchsize" in config:
            if config["fetchsize"] is not None:
                command.append(
                    "-Djdbc.fetchsize={0}".format(config["fetchsize"]))
        if "FETCH_SIZE" in config:
            if config["FETCH_SIZE"] is not None:
                command.append(
                    "-Djdbc.fetchsize={0}".format(config["FETCH_SIZE"]))
        if "check" in config:
            if config["check"] is not None:
                command.append(
                    "-Djdbc.check={0}".format(config["check"]))
    else:
        command.append(classpath)
        command.append("-Dfile.encoding=UTF-8")
    return command


def javacommand2(config, connection):
    command = []
    command.append("java")
    command.append("-classpath")
    classpath = os.environ["DATALAKE_COLLECTOR"] + \
        "lib/datalake-collector-1.0.2.jar"
    if "classpath" in config:
        if config["classpath"] is not None:
            if len(config["classpath"]) > 0:
                if platform.system() == "Windows":
                    classpath += ";"
                else:
                    classpath += ":"
                classpath += os.environ["DATALAKE_COLLECTOR"] + \
                    config["classpath"]
    if "JDBC_CLASSPATH" in config:
        if config["JDBC_CLASSPATH"] is not None:
            if len(config["JDBC_CLASSPATH"]) > 0:
                if platform.system() == "Windows":
                    classpath += ";"
                else:
                    classpath += ":"
                classpath += os.environ["DATALAKE_COLLECTOR"] + \
                    config["JDBC_CLASSPATH"]
    if "classpath" in connection:
        if connection["classpath"] is not None:
            if len(connection["classpath"]) > 0:
                if platform.system() == "Windows":
                    classpath += ";"
                else:
                    classpath += ":"
                classpath += os.environ["DATALAKE_COLLECTOR"] + \
                    connection["classpath"]
    if "JDBC_CLASSPATH" in connection:
        if connection["JDBC_CLASSPATH"] is not None:
            if len(connection["JDBC_CLASSPATH"]) > 0:
                if platform.system() == "Windows":
                    classpath += ";"
                else:
                    classpath += ":"
                classpath += os.environ["DATALAKE_COLLECTOR"] + \
                    connection["JDBC_CLASSPATH"]
    command.append(classpath)
    command.append("-Dfile.encoding=UTF-8")
    if "classname" in config:
        if config["classname"] is not None:
            if len(config["classname"]) > 0:
                command.append(
                    "-Dsource.classname={0}".format(config["classname"]))
    if "JDBC_DRIVER" in config:
        if config["JDBC_DRIVER"] is not None:
            if len(config["JDBC_DRIVER"]) > 0:
                command.append(
                    "-Dsource.classname={0}".format(config["JDBC_DRIVER"]))
    if "url" in config:
        if config["url"] is not None:
            if len(config["url"]) > 0:
                url = config["url"]
                if connection is not None:
                    url = url.format(**connection)
                command.append("-Dsource.url={0}".format(url))
    if "JDBC_URL" in config:
        if config["JDBC_URL"] is not None:
            if len(config["JDBC_URL"]) > 0:
                command.append("-Dsource.url={0}".format(config["JDBC_URL"]))
    if "username" in config:
        if config["username"] is not None:
            if len(config["username"]) > 0:
                command.append(
                    "-Dsource.username={0}".format(config["username"]))
    if "DB_USER_ID" in config:
        if config["DB_USER_ID"] is not None:
            if len(config["DB_USER_ID"]) > 0:
                command.append(
                    "-Dsource.username={0}".format(config["DB_USER_ID"]))
    if "password" in config:
        if config["password"] is not None:
            if len(config["password"]) > 0:
                command.append(
                    "-Dsource.password={0}".format(config["password"]))
    if "DB_USER_SECRET_NO" in config:
        if config["DB_USER_SECRET_NO"] is not None:
            if len(config["DB_USER_SECRET_NO"]) > 0:
                command.append(
                    "-Dsource.password={0}".format(config["DB_USER_SECRET_NO"]))
    if "fetchsize" in config:
        if config["fetchsize"] is not None:
            command.append(
                "-Dsource.fetchsize={0}".format(config["fetchsize"]))
    if "FETCH_SIZE" in config:
        if config["FETCH_SIZE"] is not None:
            command.append(
                "-Dsource.fetchsize={0}".format(config["FETCH_SIZE"]))
    if "classname" in connection:
        if connection["classname"] is not None:
            if len(connection["classname"]) > 0:
                command.append(
                    "-Ddestination.classname={0}".format(connection["classname"]))
    if "JDBC_DRIVER" in connection:
        if connection["JDBC_DRIVER"] is not None:
            if len(connection["JDBC_DRIVER"]) > 0:
                command.append(
                    "-Ddestination.classname={0}".format(connection["JDBC_DRIVER"]))
    if "url" in connection:
        if connection["url"] is not None:
            if len(connection["url"]) > 0:
                command.append(
                    "-Ddestination.url={0}".format(connection["url"]))
    if "JDBC_URL" in connection:
        if connection["JDBC_URL"] is not None:
            if len(connection["JDBC_URL"]) > 0:
                command.append(
                    "-Ddestination.url={0}".format(connection["JDBC_URL"]))
    if "username" in connection:
        if connection["username"] is not None:
            if len(connection["username"]) > 0:
                command.append(
                    "-Ddestination.username={0}".format(connection["username"]))
    if "DB_USER_ID" in connection:
        if connection["DB_USER_ID"] is not None:
            if len(connection["DB_USER_ID"]) > 0:
                command.append(
                    "-Ddestination.username={0}".format(connection["DB_USER_ID"]))
    if "password" in connection:
        if connection["password"] is not None:
            if len(connection["password"]) > 0:
                command.append(
                    "-Ddestination.password={0}".format(connection["password"]))
    if "DB_USER_SECRET_NO" in connection:
        if connection["DB_USER_SECRET_NO"] is not None:
            if len(connection["DB_USER_SECRET_NO"]) > 0:
                command.append(
                    "-Ddestination.password={0}".format(connection["DB_USER_SECRET_NO"]))
    return command


def make_column(columns):
    result = []
    for column in columns:
        entry = {}
        entry["DB_TABLE_ATRB_SN"] = jdbcutil.nvlint(column, "DB_TABLE_ATRB_SN")
        entry["TABLE_ATRB_ENG_NM"] = jdbcutil.nvl(
            column, "TABLE_ATRB_ENG_NM", "")
        entry["TABLE_KOREAN_ATRB_NM"] = jdbcutil.nvl(
            column, "TABLE_KOREAN_ATRB_NM", "")
        entry["TABLE_ATRB_EXPR"] = jdbcutil.nvl(column, "TABLE_ATRB_EXPR", "")
        entry["DSTNG_TRGET_AT"] = jdbcutil.nvl(column, "DSTNG_TRGET_AT", "")
        entry["TABLE_ATRB_TY_NM"] = jdbcutil.nvl(
            column, "TABLE_ATRB_TY_NM", "")
        entry["TABLE_ATRB_LT_VALUE"] = jdbcutil.nvl(
            column, "TABLE_ATRB_LT_VALUE", "")
        entry["HIVE_COL_NM"] = jdbcutil.nvl(column, "HIVE_COL_NM", "")
        entry["HIVE_ATRB_TY_NM"] = jdbcutil.nvl(column, "HIVE_ATRB_TY_NM", "")
        entry["TABLE_ATRB_NULL_POSBL_AT"] = jdbcutil.nvl(
            column, "TABLE_ATRB_NULL_POSBL_AT", "Y")
        entry["TABLE_ATRB_PK_AT"] = jdbcutil.nvl(
            column, "TABLE_ATRB_PK_AT", "N")
        result.append(entry)
    return result


def make_create_query(connection, table, columns):
    query = []
    columndef = []
    partition = []
    query.append("CREATE TABLE IF NOT EXISTS " +
                 jdbcutil.hivetablename(connection, table) + " (")
    part_cols = table["PART_COLS"]
    for column in columns:
        if column["TABLE_ATRB_ENG_NM"] in table["PART_COLS"]:
            part_cols.remove(column["TABLE_ATRB_ENG_NM"])
            partition.append(jdbcutil.hivecolumndef(column))
        else:
            columndef.append(jdbcutil.hivecolumndef(column))
    for col in part_cols:
        column = {"DB_TABLE_ATRB_SN": None,
                  "TABLE_ATRB_ENG_NM": col,
                  "TABLE_KOREAN_ATRB_NM": "배치기준시간",
                  "TABLE_ATRB_EXPR": "'{BATCH_DATE}'",
                  "DSTNG_TRGET_AT": "",
                  "TABLE_ATRB_TY_NM": "STRING",
                  "TABLE_ATRB_LT_VALUE": "",
                  "HIVE_COL_NM": col,
                  "HIVE_ATRB_TY_NM": "STRING",
                  "TABLE_ATRB_NULL_POSBL_AT": "Y",
                  "TABLE_ATRB_PK_AT": "N"}
        partition.append(jdbcutil.hivecolumndef(column))
        columns.append(column)
    for column in columndef:
        if len(query) > 1:
            if not query[-1].endswith("("):
                query[-1] += ","
        query.append("    {0}".format(column))
    query.append(")")
    constraint = jdbcutil.hivetableconstraint(
        connection["DB_TY_NM"], table, columns)
    if constraint is not None:
        query.append(constraint)
    if len(partition) > 0:
        query.append("PARTITIONED BY (")
        for column in partition:
            if len(query) > 1:
                if not query[-1].endswith("("):
                    query[-1] += ","
            query.append("    {0}".format(column))
        query.append(")")
    query.append(jdbcutil.hiverowformat())
    query.append(jdbcutil.hivestoredas())
    return query


def make_drop_query(connection, table):
    query = []
    query.append("DROP TABLE IF EXISTS {0}".format(
        jdbcutil.hivetablename(None, table)))
    return query


def make_truncate_query(connection, table):
    query = []
    query.append("TRUNCATE TABLE {0}".format(
        jdbcutil.hivetablename(connection, table)))
    return query


def make_use_query(connection, table):
    query = []
    query.append("USE {0}".format(connection["HIVE_DB_NM"]))
    return query


def make_connection_config(conf, connection, tables={}):
    config = {}
    config["CNNC_MANAGE_NO"] = jdbcutil.nvl(connection, "CNNC_MANAGE_NO", "")
    config["SYS_NM"] = jdbcutil.nvl(connection, "SYS_NM", "")
    config["CNTC_MTHD_CODE"] = jdbcutil.nvl(connection, "CNTC_MTHD_CODE", "")
    config["CNTC_MTHD_NM"] = jdbcutil.nvl(
        connection, "CNTC_MTHD_NM", conf["cntc_mthd_nm"].get(config["CNTC_MTHD_CODE"]))
    config["DB_TY_CODE"] = jdbcutil.nvl(
        connection, "DB_TY_CODE", jdbcutil.dbtycode(connection))
    config["DB_TY_NM"] = jdbcutil.nvl(connection, "DB_TY_NM", "GENERIC")
    config["DB_ACNT_NM"] = jdbcutil.nvl(connection, "DB_ACNT_NM", "")
    config["HIVE_DB_NM"] = jdbcutil.nvl(connection, "HIVE_DB_NM", "")
    config["DB_SERVICE_NM"] = jdbcutil.nvl(connection, "DB_SERVICE_NM", "")
    config["DB_1_SERVER_IP"] = jdbcutil.nvl(connection, "DB_1_SERVER_IP", "")
    config["DB_1_SERVER_PORT_NO"] = jdbcutil.nvl(
        connection, "DB_1_SERVER_PORT_NO", "")
    config["DB_2_SERVER_IP"] = jdbcutil.nvl(connection, "DB_2_SERVER_IP", "")
    config["DB_2_SERVER_PORT_NO"] = jdbcutil.nvl(
        connection, "DB_2_SERVER_PORT_NO", "")
    config["DB_USER_ID"] = jdbcutil.nvl(connection, "DB_USER_ID")
    config["DB_USER_SECRET_NO"] = jdbcutil.nvl(connection, "DB_USER_SECRET_NO")
    config["REMOTE_SERVER_IP"] = jdbcutil.nvl(
        connection, "REMOTE_SERVER_IP", "")
    config["REMOTE_SERVER_PORT_NO"] = jdbcutil.nvl(
        connection, "REMOTE_SERVER_PORT_NO", "")
    config["REMOTE_SERVER_USER_ID"] = jdbcutil.nvl(
        connection, "REMOTE_SERVER_USER_ID", "")
    config["REMOTE_SERVER_USER_SECRET_NO"] = jdbcutil.nvl(
        connection, "REMOTE_SERVER_USER_SECRET_NO", "")
    config["REMOTE_DRCTRY_NM"] = jdbcutil.nvl(connection, "DATA_TY_CODE", "")
    config["DATA_TY_CODE"] = jdbcutil.nvl(connection, "DATA_TY_CODE", "")
    config["API_DATA_AUTHKEY_NM"] = jdbcutil.nvl(
        connection, "API_DATA_AUTHKEY_NM", "")
    config["API_DATA_URL"] = jdbcutil.nvl(connection, "API_DATA_URL", "")
    config["NTWK_SE_CODE"] = jdbcutil.nvl(connection, "NTWK_SE_CODE", "")
    config["NTWK_SE_NM"] = jdbcutil.nvl(connection, "NTWK_SE_NM", "")
    config["APPLC_SE_CODE"] = jdbcutil.nvl(connection, "APPLC_SE_CODE", "")
    config["APPLC_SE_NM"] = jdbcutil.nvl(connection, "APPLC_SE_NM", "")
    config["JDBC_CLASSPATH"] = jdbcutil.classpath(connection)
    config["JDBC_DRIVER"] = jdbcutil.classname(connection)
    config["JDBC_URL"] = jdbcutil.url(connection)
    config["FETCH_SIZE"] = jdbcutil.nvlint(conf, "FETCH_SIZE", 1000)
    config["TABLE_NM"] = []
    for key in sorted(tables.keys()):
        config["TABLE_NM"].append(tables[key]["TABLE_ENG_NM"])
    file = open(
        "cfg/IM-{0}/SYS-{0}.json".format(config["CNNC_MANAGE_NO"]), "w", encoding="utf-8")
    file.write(json.dumps(config, indent=4, sort_keys=True, ensure_ascii=False))
    file.close()


def make_table_config(conf, connection, table, columns):
    config = {}
    config["CNNC_MANAGE_NO"] = jdbcutil.nvl(table, "CNNC_MANAGE_NO", "")
    config["TABLE_ENG_NM"] = jdbcutil.nvl(table, "TABLE_ENG_NM", "")
    config["TABLE_KOREAN_NM"] = jdbcutil.nvl(table, "TABLE_KOREAN_NM", "")
    config["PART_COLS"] = jdbcutil.nvl(table, "PART_COLS")
    config["PART_COLS"] = []
    if table["PART_COLS"] is not None:
        if len(table["PART_COLS"]) > 0:
            config["PART_COLS"] = table["PART_COLS"].split(",")
    config["WHERE_INFO_NM"] = jdbcutil.nvl(table, "WHERE_INFO_NM", "")
    config["INS_NUM_MAPPERS"] = jdbcutil.nvl(table, "INS_NUM_MAPPERS", "")
    config["INS_SPLIT_BY_COL"] = jdbcutil.nvl(table, "INS_SPLIT_BY_COL", "")
    config["APD_CHK_COL"] = jdbcutil.nvl(table, "APD_CHK_COL", "")
    config["APD_WHERE"] = jdbcutil.nvl(table, "APD_WHERE", "")
    config["HIVE_TABLE_NM"] = jdbcutil.nvl(table, "HIVE_TABLE_NM", "")
    config["BIGDATA_GTRN_AT"] = jdbcutil.nvl(table, "BIGDATA_GTRN_AT", "")
    config["SCHDUL_APPLC_AT"] = jdbcutil.nvl(table, "SCHDUL_APPLC_AT", "")
    config["GTHNLDN_MTH_CODE"] = jdbcutil.nvl(table, "GTHNLDN_MTH_CODE", "")
    config["GTHNLDN_MTH_NM"] = jdbcutil.nvl(table, "GTHNLDN_MTH_NM", "")
    config["COLUMNS"] = make_column(columns)
    config["QUERY"] = {"CREATE": None, "DROP": None, "TRUNCATE": None}
    config["QUERY"]["CREATE"] = make_create_query(
        connection, config, config["COLUMNS"])
    config["QUERY"]["DROP"] = make_drop_query(connection, config)
    config["QUERY"]["TRUNCATE"] = make_truncate_query(connection, config)
    config["QUERY"]["USE"] = make_use_query(connection, config)
    file = open(
        "cfg/IM-{0}/TAB-{0}-{1}.json".format(connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]), "w", encoding="utf-8")
    file.write(json.dumps(config, indent=4, sort_keys=True, ensure_ascii=False))
    file.close()
