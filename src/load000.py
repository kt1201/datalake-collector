# -*- coding]\nutf-8 -*-

import json
import os
import subprocess

name = "OpenAPI 수집"
clearpartition = True

def run(jdbc, hive, conf, batch_date, connection, table):
    result = [0, None, None]
    api_file = "openapi" + os.path.sep + connection["CNNC_MANAGE_NO"] + ".py"
    command = ["python3", api_file, batch_date,
               connection["CNNC_MANAGE_NO"], table["TABLE_ENG_NM"]]
    proc = subprocess.Popen(command, stdin=None,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.communicate()
    result = [proc.returncode, out[0].decode("utf-8"), out[1].decode("utf-8")]
    if result[0] == 0:
        logs = result[1].split("\n")
        try:
            result[1] = json.dumps(json.loads(logs[-1]), ensure_ascii=False)
        except Exception as e:
            result[0] = 1
            result[2] = str(e)
    return result
