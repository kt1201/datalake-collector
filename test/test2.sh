#!/bin/bash

echo "==== 저장소 자동 생성 테스트 ===="

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
hivecreatetable L001 >out/hivecreatetable_201.out 2>/dev/null
checkout "Hive 테이블 생성" "hivecreatetable_201"

# Hive 파티션 목록 1
hivelistpartition L001 >out/hivelistpartition_201.out 2>/dev/null
checkout "Hive 파티션 목록 1" "hivelistpartition_201"

# Hive 파티션 생성(20200101)
hivecreatepartition 20200101000000 L001 >out/hivecreatepartition_201.out 2>/dev/null
checkout "Hive 파티션 생성(20200101)" "hivecreatepartition_201"

# Hive 파티션 목록 2
hivelistpartition L001 >out/hivelistpartition_202.out 2>/dev/null
checkout "Hive 파티션 목록 2" "hivelistpartition_202"

# Hive 파티션 생성(20200201)
hivecreatepartition 20200201000000 L001 >out/hivecreatepartition_202.out 2>/dev/null
checkout "Hive 파티션 생성(20200201)" "hivecreatepartition_202"

# Hive 파티션 목록 3
hivelistpartition L001 >out/hivelistpartition_203.out 2>/dev/null
checkout "Hive 파티션 목록 3" "hivelistpartition_203"

# Hive 테이블 초기화
hivetruncatetable L001 >out/hivetruncatetable_201.out 2>/dev/null
checkout "Hive 테이블 초기화" "hivetruncatetable_201"

# Hive 파티션 정리(20200101)
hiveclearpartition 20200101000000 L001 >out/hiveclearpartition_201.out 2>/dev/null
checkout "Hive 파티션 정리(20200101)" "hiveclearpartition_201"

# Hive 파티션 목록 4
hivelistpartition L001 >out/hivelistpartition_204.out 2>/dev/null
checkout "Hive 파티션 목록 4" "hivelistpartition_204"

# Hive 파티션 삭제(20200101)
hivedroppartition 20200101000000 L001 >out/hivedroppartition_201.out 2>/dev/null
checkout "Hive 파티션 삭제(20200101)" "hivedroppartition_201"

# Hive 파티션 목록 5
hivelistpartition L001 >out/hivelistpartition_205.out 2>/dev/null
checkout "Hive 파티션 목록 5" "hivelistpartition_205"

# Hive 테이블 삭제
hivedroptable L001 >out/hivedroptable_201.out 2>/dev/null
checkout "Hive 테이블 삭제" "hivedroptable_201"
