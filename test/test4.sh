#!/bin/bash

echo "==== OPENAPI 테스트 ===="

# 결과 비교 함수
function checkout {
    diff "out/$2.out" "lst/$2.lst"
    if [ $? = 0 ]; then
        echo "[성공] $1"
    else
        echo "[실패] $1"
    fi
}

# OPENAPI 적재(template1)
../openapi/template1.py 20200101000000 template1 getBeachLocationList >out/openapi_401.out 2>/dev/null
checkout "OPENAPI 적재(template1)" "openapi_401"

# OPENAPI 적재(template2)
../openapi/template2.py 20200101000000 template2 >out/openapi_402.out 2>/dev/null
checkout "OPENAPI 적재(template2)" "openapi_402"

# OPENAPI 적재(template3)
../openapi/template3.py 20200101000000 template3 >out/openapi_403.out 2>/dev/null
checkout "OPENAPI 적재(template3)" "openapi_403"

# OPENAPI 적재 검증(template1)
hiveverify 20200101000000 template1 >out/hiveverify_401.out 2>/dev/null
checkout "OPENAPI 적재 검증(template1)" "hiveverify_401"

# OPENAPI 적재 검증(template3)
hiveverify 20200101000000 template3 >out/hiveverify_402.out 2>/dev/null
checkout "OPENAPI 적재 검증(template3)" "hiveverify_402"

# HIVE to JDBC 수집/적재
hive2jdbc template1 >out/hive2jdbc_401.out 2>/dev/null
checkout "HIVE to JDBC 수집/적재" "hive2jdbc_401"

# Hive 테이블 삭제(template1)
hivedroptable template1 >out/hivedroptable_401.out 2>/dev/null
checkout "Hive 테이블 삭제(template1)" "hivedroptable_401"

# Hive 테이블 삭제(template2)
hivedroptable template2 >out/hivedroptable_402.out 2>/dev/null
checkout "Hive 테이블 삭제(template2)" "hivedroptable_402"

# Hive 테이블 삭제(template3)
hivedroptable template3 >out/hivedroptable_403.out 2>/dev/null
checkout "Hive 테이블 삭제(template3)" "hivedroptable_403"
