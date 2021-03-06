#!/bin/bash

echo "==== 수집/적재 테스트 ===="

# 결과 비교 함수
function checkout {
    diff "out/$2.out" "lst/$2.lst"
    if [ $? = 0 ]; then
        echo "[성공] $1"
    else
        echo "[실패] $1"
    fi
}

# Hive 테이블 생성
hivecreatetable L001 >out/hivecreatetable_301.out 2>/dev/null
checkout "Hive 테이블 생성" "hivecreatetable_301"

# Hive 파티션 생성(20200101)
hivecreatepartition 20200101000000 L001 >out/hivecreatepartition_301.out 2>/dev/null
checkout "Hive 파티션 생성(20200101)" "hivecreatepartition_301"

# JDBC to SequenceFile 수집/적재
jdbc2seqfile 20200101000000 L001 >out/jdbc2seqfile_301.out 2>/dev/null
checkout "JDBC to SequenceFile 수집/적재" "jdbc2seqfile_301"

# Hive 파티션 생성(20200201)
hivecreatepartition 20200201000000 L001 >out/hivecreatepartition_302.out 2>/dev/null
checkout "Hive 파티션 생성(20200201)" "hivecreatepartition_302"

# CSV to SEQUENCEFILE 적재
csv2seqfile 20200201000000 L001 t_eplog.csv t_eplog >out/csv2seqfile_301.out 2>/dev/null
checkout "CSV to SEQUENCEFILE 적재" "csv2seqfile_301"

# JDBC to JDBC 수집/적재
jdbc2jdbc 20200301000000 L001 t_eplog R001 T_EPLOG >out/jdbc2jdbc_301.out 2>/dev/null
checkout "JDBC to JDBC 수집/적재" "jdbc2jdbc_301"

# Hive 테이블 분석
hiveanalyze 20200101000000 L001 >out/hiveanalyze_301.out 2>/dev/null
checkout "Hive 테이블 분석" "hiveanalyze_301"

# JDBC 레코드 수
jdbccount 20200101000000 L001 >out/jdbccount_301.out 2>/dev/null
checkout "JDBC 레코드 수" "jdbccount_301"

# Hive 레코드 수
hivecount 20200101000000 L001 >out/hivecount_301.out 2>/dev/null
checkout "Hive 레코드 수" "hivecount_301"

# 비식별화 검증
jdbc2csv 20200101000000 L001 t_eplog out/t_eplog_301.out >/dev/null 2>/dev/null
checkout "비식별화 검증" "t_eplog_301"

# Hive 적재 데이터 검증
hive2csv 20200101000000 L001 t_eplog out/t_eplog_302.out >/dev/null 2>/dev/null
checkout "Hive 적재 데이터 검증" "t_eplog_302"

# JDBC to JDBC 수집/적재 검증
jdbc2csv 20200301000000 R001 T_EPLOG out/t_eplog_303.out >/dev/null 2>/dev/null
checkout "JDBC to JDBC 수집/적재 검증" "t_eplog_303"

# CSV to SEQUENCEFILE 적재 검증
hivequery L001 "SELECT * FROM T_EPLOG WHERE PART_BATCHDATE = CAST(20200201000000 AS STRING)" >out/hivequery_301.out 2>/dev/null
checkout "CSV to SEQUENCEFILE 적재 검증" "hivequery_301"

# JDBC to SequenceFile 적재 검증
hiveverify 20200101000000 L001 >out/hiveverify_301.out 2>/dev/null
checkout "JDBC to SequenceFile 적재 검증" "hiveverify_301"

# Target 테이블 초기화
jdbctruncatetable R001 >out/jdbctruncatetable_301.out 2>/dev/null
checkout "Target 테이블 초기화" "jdbctruncatetable_301"

# Target 테이블 조회
jdbcquery R001 "SELECT * FROM T_EPLOG" >out/jdbcquery_301.out 2>/dev/null
checkout "Target 테이블 조회" "jdbcquery_301"

# Hive 테이블 삭제
hivedroptable L001 >out/hivedroptable_301.out 2>/dev/null
checkout "Hive 테이블 삭제" "hivedroptable_301"
