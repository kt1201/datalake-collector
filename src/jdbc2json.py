# -*- coding: utf-8 -*-

import json
import subprocess
import sys

import jdbcconf


class Connection:

    def __init__(self, config, connection={"HIVE_DB_NM": ""}):
        command = jdbcconf.javacommand(config, connection)
        command.append("kr.co.penta.datalake.jdbc2json.Application")
        self._jdbc2json = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self._meta = None
        self._result = None
        packet = json.dumps({"type": "check"}, ensure_ascii=False) + "\n"
        packet = bytes(packet, "utf-8")
        self._jdbc2json.stdin.write(packet)
        self._jdbc2json.stdin.flush()
        self._result = json.loads(self._jdbc2json.stdout.readline())

    def __del__(self):
        if self._jdbc2json is not None:
            self._jdbc2json.stdin.close()
            self._jdbc2json.wait()
            if self._jdbc2json.returncode != 0:
                print("jdbc2json.returncode = {0}".format(
                    self._jdbc2json.returncode), file=sys.stderr, flush=True)
            self._jdbc2json = None
        self._meta = None
        self._result = None

    def _execute(self, packet):
        self._result = None
        packet = bytes(packet, "utf-8")
        self._jdbc2json.stdin.write(packet)
        self._jdbc2json.stdin.flush()
        self._result = json.loads(self._jdbc2json.stdout.readline())
        return self._result

    def disconnect(self):
        self._meta = None
        packet = json.dumps({"type": "disconnect"}, ensure_ascii=False) + "\n"
        self._execute(packet)

    def statement(self, query):
        self._meta = None
        packet = json.dumps(
            {"type": "statement", "query": query}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return Statement(self)

    def open(self, query, binds=[], csv=False, header=False, meta=True):
        self._meta = None
        statement = self.statement(query)
        resultset = None
        try:
            resultset = statement.open(binds, csv, header, meta)
            self._meta = resultset._meta
        except Exception as e:
            self._result = {"Exception": str(e)}
        return resultset

    def getautocommit(self):
        self._meta = None
        packet = json.dumps({"type": "getautocommit"},
                            ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result["autocommit"]

    def setautocommit(self, autocommit):
        self._meta = None
        packet = json.dumps(
            {"type": "setautocommit", "autocommit": autocommit}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)

    def commit(self):
        self._meta = None
        packet = json.dumps({"type": "commit"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)

    def rollback(self):
        self._meta = None
        packet = json.dumps({"type": "rollback"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)

    def execute(self, query, binds=[], csv=False, header=False, meta=True):
        self._meta = None
        packet = json.dumps({"type": "execute", "query": query, "binds": binds,
                             "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def execute_query(self, query, binds=[], csv=False, header=False, meta=True):
        result = self.execute(query, binds, csv, header, meta)
        if not isinstance(result, list):
            raise Exception("쿼리 결과 형식 오류: {0}".format(result))
        return result

    def execute_update(self, query, binds=[]):
        result = self.execute(query, binds)
        if not isinstance(result, dict):
            raise Exception("실행 결과 형식 오류: {0}".format(result))
        if "UpdateCount" not in result:
            raise Exception("실행 결과 형식 오류: {0}".format(result))
        return result["UpdateCount"]

    def execute_list(self, query, binds=[[]]):
        self._meta = None
        packet = json.dumps(
            {"type": "execute_list", "query": query, "binds": binds}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def database(self):
        self._meta = None
        packet = json.dumps({"type": "database"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def username(self):
        self._meta = None
        packet = json.dumps({"type": "username"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result["UserName"]

    def catalog(self):
        self._meta = None
        packet = json.dumps({"type": "catalog"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result["Catalog"]

    def catalogs(self):
        self._meta = None
        packet = json.dumps({"type": "catalogs"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def schemas(self, catalog=None, schema=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "schemas", "catalog": catalog, "schema": schema,
                             "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def tables(self, catalog=None, schema=None, table=None, types=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "tables", "catalog": catalog, "schema": schema, "table": table,
                             "types": types, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def columns(self, catalog=None, schema=None, table=None, column=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "columns", "catalog": catalog, "schema": schema, "table": table,
                             "column": column, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def indices(self, catalog=None, schema=None, table=None, unique=False, approximate=False, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "indices", "catalog": catalog, "schema": schema, "table": table, "unique": unique,
                             "approximate": approximate, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def indexes(self, catalog=None, schema=None, table=None, unique=False, approximate=False, csv=False, header=True, meta=True):
        return self.indices(catalog, schema, table, unique, approximate, csv, header, meta)

    def primarykeys(self, catalog=None, schema=None, table=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "primarykeys", "catalog": catalog, "schema": schema,
                             "table": table, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def exportedkeys(self, catalog=None, schema=None, table=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "exportedkeys", "catalog": catalog, "schema": schema,
                             "table": table, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def importedkeys(self, catalog=None, schema=None, table=None, csv=False, header=True, meta=True):
        self._meta = None
        packet = json.dumps({"type": "importedkeys", "catalog": catalog, "schema": schema,
                             "table": table, "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def keywords(self):
        self._meta = None
        packet = json.dumps({"type": "keywords"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def tabletypes(self):
        self._meta = None
        packet = json.dumps({"type": "tabletypes"}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def meta(self):
        if self._meta is None:
            return []
        return self._meta

    def count(self, result=None):
        if result is None:
            result = self._result
        if result is not None:
            if isinstance(result, dict):
                if "UpdateCount" in result:
                    return result["UpdateCount"]
            if isinstance(result, list):
                return len(result)
        return 0

    def error(self, result=None):
        if result is None:
            result = self._result
        if result is not None:
            if isinstance(result, dict):
                if "Exception" in result:
                    return result["Exception"]
            elif not isinstance(result, list):
                return str(result)
        return None


class Statement:

    def __init__(self, connection):
        self._connection = connection
        self._id = connection._result["id"]
        self._meta = None
        self._result = connection._result

    def __del__(self):
        if self._connection is not None:
            packet = json.dumps(
                {"type": "free", "id": self._id}, ensure_ascii=False) + "\n"
            self._connection._execute(packet)
            error = self._connection.error()
            if error != None:
                print("Statement: {0}".format(error),
                      file=sys.stderr, flush=True)
            self._connection = None
        self._id = None
        self._meta = None
        self._result = None

    def _execute(self, packet):
        self._result = None
        self._result = self._connection._execute(packet)
        return self._result

    def open(self, binds=[], csv=False, header=False, meta=True):
        self._meta = None
        packet = json.dumps({"type": "open", "id": self._id, "binds": binds,
                             "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result["result"], list):
            self._meta = self._result["result"].pop(0)
        resultset = ResultSet(self)
        self._result = resultset._result
        return resultset

    def execute(self, binds=[], csv=False, header=False, meta=True):
        self._meta = None
        packet = json.dumps({"type": "execute", "id": self._id, "binds": binds,
                             "csv": csv, "header": header, "meta": meta}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        if meta == True and isinstance(self._result, list):
            self._meta = self._result.pop(0)
        return self._result

    def execute_query(self, binds=[], csv=False, header=False, meta=True):
        result = self.execute(binds, csv, header, meta)
        if not isinstance(result, list):
            raise Exception("쿼리 결과 형식 오류: {0}".format(result))
        return result

    def execute_update(self, binds=[]):
        result = self.execute(binds)
        if not isinstance(result, dict):
            raise Exception("실행 결과 형식 오류: {0}".format(result))
        if "UpdateCount" not in result:
            raise Exception("실행 결과 형식 오류: {0}".format(result))
        return result["UpdateCount"]

    def execute_list(self, binds=[[]]):
        self._meta = None
        packet = json.dumps({"type": "execute_list", "id": self._id,
                             "binds": binds}, ensure_ascii=False) + "\n"
        self._execute(packet)
        error = self.error()
        if error is not None:
            raise Exception(error)
        return self._result

    def count(self, result=None):
        if result is None:
            result = self._result
        if result is not None:
            if isinstance(result, dict):
                if "UpdateCount" in result:
                    return result["UpdateCount"]
            if isinstance(result, list):
                return len(result)
        return 0

    def meta(self):
        if self._meta is None:
            return []
        return self._meta

    def error(self, result=None):
        if result is None:
            result = self._result
        if result is not None:
            if isinstance(result, dict):
                if "Exception" in result:
                    return result["Exception"]
            elif not isinstance(result, list):
                return str(result)
        return None


class ResultSet:

    def __init__(self, statement):
        self._statement = statement
        self._id = statement._result["id"]
        self._meta = statement._meta
        self._result = statement._result["result"]

    def __del__(self):
        if self._statement is not None:
            packet = json.dumps(
                {"type": "close", "id": self._id}, ensure_ascii=False) + "\n"
            self._statement._execute(packet)
            error = self._statement.error()
            if error != None:
                print("ResultSet: {0}".format(error),
                      file=sys.stderr, flush=True)
            self._statement = None
        self._id = None
        self._meta = None
        self._result = None

    def fetch(self):
        if self._result is None and self._id is not None:
            packet = json.dumps(
                {"type": "fetch", "id": self._id}, ensure_ascii=False) + "\n"
            self._result = self._statement._execute(packet)
            error = self.error()
            if error != None:
                raise Exception(error)
            if len(self._result) <= 0:
                packet = json.dumps(
                    {"type": "close", "id": self._id}, ensure_ascii=False) + "\n"
                self._result = self._statement._execute(packet)
                error = self.error()
                if error != None:
                    raise Exception(error)
                self._statement = None
                self._id = None
                self._result = None
        if self._result is not None:
            if len(self._result) > 0:
                record = self._result.pop(0)
                if len(self._result) <= 0:
                    self._result = None
                return record
        return None

    def meta(self):
        if self._meta is None:
            return []
        return self._meta

    def error(self, result=None):
        if result is None:
            result = self._result
        if result is not None:
            if isinstance(result, dict):
                if "Exception" in result:
                    return result["Exception"]
            elif not isinstance(result, list):
                return str(result)
        return None


def new(config, connection={"HIVE_DB_NM": ""}):
    return Connection(config, connection)


def command(config, query):
    command = jdbcconf.javacommand(config)
    command.append("kr.co.penta.datalake.jdbc2json.Application")
    command.append(query)
    proc = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        out = proc.communicate(timeout=60)
        if proc.returncode == 0:
            return json.loads(out[0].decode("utf-8"))
    except Exception as e:
        proc.kill()
        print("Exception: {0}".format(e), flush=True)
    return None
