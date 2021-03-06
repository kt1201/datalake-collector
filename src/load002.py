# -*- coding]\nutf-8 -*-

import json

import jdbcutil
import hivecreatetable
import hivetruncatetable
import jdbc2seqfile
import hiveanalyze
import hiveverify

name = "Truncate후 수집/적재"
clearpartition = False
handlers = [hivecreatetable, hivetruncatetable,
            jdbc2seqfile, hiveanalyze, hiveverify]


def run(jdbc, hive, conf, batch_date, connection, table):
    result = [0, None, None]
    status = {}
    for handler in handlers:
        result = handler.run(jdbc, hive, conf, batch_date,
                             connection, table, None)
        if result[0] != 0:
            result[2] = "[{0}]\n{1}".format(handler.name, result[2])
            return result
        status = jdbcutil.merge(status, result[1])
    result[1] = json.dumps(status, ensure_ascii=False)
    result[2] = None
    return result
