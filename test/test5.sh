#!/bin/bash

echo "==== 배치 테스트 ===="

# 결과 비교 함수
function checkout {
    diff "out/$2.out" "lst/$2.lst"
    if [ $? = 0 ]; then
        echo "[성공] $1"
    else
        echo "[실패] $1"
    fi
}

# 내부 배치
batchrun 20200101000000 01 L001 M001 >out/batchrun_501.out 2>/dev/null
echo "$?" >>out/batchrun_501.out
checkout "내부 배치" "batchrun_501"

# 외부 배치
batchrun 20200101000000 02 template1 template2 template3 >out/batchrun_502.out 2>/dev/null
echo "$?" >>out/batchrun_502.out
checkout "외부 배치" "batchrun_502"

# 일별 테이블 적재
../src/load004.py 20200101000000 L001 daily_% >out/load004_501.out 2>/dev/null
echo "$?" >>out/load004_501.out
checkout "일별 테이블 적재" "load004_501"

# 내부 배치(재시도)
batchrun 20200101000000 01 L001 M001 >out/batchrun_503.out 2>/dev/null
echo "$?" >>out/batchrun_503.out
checkout "내부 배치(재시도)" "batchrun_503"

# 배치 결과(내부)
batchresult 20200101000000 01 >out/batchresult_501.out 2>/dev/null
echo "$?" >>out/batchresult_501.out
checkout "배치 결과(내부)" "batchresult_501"

# 배치 결과(외부)
batchresult 20200101000000 02 >out/batchresult_502.out 2>/dev/null
echo "$?" >>out/batchresult_502.out
checkout "배치 결과(외부)" "batchresult_502"

# 배치 검사
batchcheck 20200101000000 L001 template1 template2 template3 >out/batchcheck_501.out 2>/dev/null
echo "$?" >>out/batchcheck_501.out
checkout "배치 검사" "batchcheck_501"

# 결과 검증
sqlite3 data/ruledb.db 'select planned_dt, cntc_mthd_nm, cnnc_manage_no, table_eng_nm, status, records, accumulated_records, increased_accumulated_records, verify from t_batch_result_0003 order by 1, 2, 3, 4' >out/result_501.out 2>&1
checkout "결과 검증" "result_501"
