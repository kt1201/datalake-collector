# -*- coding: utf-8 -*-

import base64
import contextlib
import datetime
import json


def escape_token(token):
    if token is None:
        return ""
    if isinstance(token, datetime.datetime):
        value = token.strftime("%Y-%m-%d %H:%M:%S.%f")
        while value.endswith("0") and not value.endswith(".0"):
            value = value[:-1]
        return value
    if isinstance(token, datetime.date):
        value = token.strftime("%Y-%m-%d")
        return value
    if isinstance(token, datetime.time):
        value = token.strftime("%H:%M:%S.%f")
        while value.endswith("0") and not value.endswith(".0"):
            value = value[:-1]
        return value
    if isinstance(token, str):
        return "\""+token.replace("\"", "\"\"")+"\""
    if isinstance(token, (bytes, bytearray)):
        return "="+base64.b64encode(token).decode("UTF-8")
    if isinstance(token, float):
        if token == int(token):
            token = int(token)
    return json.dumps(token, ensure_ascii=False)


def unescape_token(token):
    if len(token) == 0 or token == "\\N":
        return None
    if token.startswith("="):
        return base64.b64decode(token[1:])
    if token.startswith("\"") and token.endswith("\""):
        return token[1:len(token)-1].replace("\"\"", "\"")
    with contextlib.suppress(ValueError):
        return datetime.datetime.strptime(token, "%Y-%m-%d %H:%M:%S.%f")
    with contextlib.suppress(ValueError):
        return datetime.datetime.strptime(token, "%Y-%m-%d %H:%M:%S")
    with contextlib.suppress(ValueError):
        value = datetime.datetime.strptime(token, "%Y-%m-%d")
        return datetime.date(value.year, value.month, value.day)
    with contextlib.suppress(ValueError):
        value = datetime.datetime.strptime(token, "%H:%M:%S.%f")
        return datetime.time(value.hour, value.minute, value.second, value.microsecond)
    with contextlib.suppress(ValueError):
        value = datetime.datetime.strptime(token, "%H:%M:%S")
        return datetime.time(value.hour, value.minute, value.second)
    with contextlib.suppress(json.JSONDecodeError):
        return json.loads(token)
    return token


def writerecord(file, record, columns=None):
    length = len(record)
    if columns is None:
        columns = length
    for index in range(columns):
        if index > 0:
            file.write(",")
        if index < length:
            file.write(escape_token(record[index]))
        else:
            file.write(escape_token(None))
    file.write("\n")


def readrecord(file, columns=None):
    record = []
    value = ""
    ch = file.read(1)
    if ch == "":
        return None
    while ch != "":
        if ch == "\n":
            break
        if ch == ",":
            record.append(unescape_token(value))
            value = ""
        elif ch == "\"":
            value += ch
            ch = file.read(1)
            while ch != "":
                value += ch
                if ch == "\"":
                    break
                ch = file.read(1)
        else:
            value += ch
        ch = file.read(1)
    record.append(unescape_token(value))
    if columns is not None:
        if len(record) > columns:
            record = record[0:columns]
        while len(record) < columns:
            record.append(None)
    return record


def writeall(file, records, columns=None):
    for record in records:
        writerecord(file, record, columns)


def readall(file, columns=None):
    records = []
    record = readrecord(file, columns)
    while record is not None:
        records.append(record)
        record = readrecord(file, columns)
    return records


def writefile(filename, records, columns=None):
    file = open(filename, "w", encoding="utf-8")
    writeall(file, records, columns)
    file.close()


def readfile(filename, columns=None):
    file = open(filename, "r", encoding="utf-8")
    csv = readall(file)
    file.close()
    return csv


def csv2dict(csv, header=None):
    result = []
    if header is None:
        header = csv[0]
        values = csv[1:]
    else:
        values = csv
    for value in values:
        entry = {}
        for index in range(len(header)):
            if index < len(value):
                entry[header[index]] = value[index]
        result.append(entry)
    return result


def file2dict(filename):
    return csv2dict(readfile(filename))


def dict2keys(records, keys):
    result = {}
    for record in records:
        value = result
        for key in keys:
            if record[key] not in value:
                value[record[key]] = {}
            value = value[record[key]]
        if value == {}:
            value = value.update(record)
        else:
            for key in record.keys():
                if record[key] is not None:
                    value[key] = record[key]
    return result
