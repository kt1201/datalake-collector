#!/bin/bash

echo "==== 설정 파일 생성 테스트 ===="

# 결과 비교 함수
function checkout {
    diff "out/$2.out" "lst/$2.lst"
    if [ $? = 0 ]; then
        echo "[성공] $1"
    else
        echo "[실패] $1"
    fi
}

# 설정 파일 생성(L001)
buildcfg L001 >out/buildcfg_101.out 2>&1
checkout "설정 파일 생성(L001)" "buildcfg_101"

# 설정 파일 생성(R001)
buildcfg R001 >out/buildcfg_102.out 2>&1
checkout "설정 파일 생성(R001)" "buildcfg_102"

# 설정 파일 생성(template1)
buildcfg template1 >out/buildcfg_103.out 2>&1
checkout "설정 파일 생성(template1)" "buildcfg_103"

# 설정 파일 생성(template2)
buildcfg template2 >out/buildcfg_104.out 2>&1
checkout "설정 파일 생성(template2)" "buildcfg_104"

# 설정 파일 생성(template3)
buildcfg template3 >out/buildcfg_105.out 2>&1
checkout "설정 파일 생성(template3)" "buildcfg_105"

# JDBC Version 조회
jdbcversion L001 R001 >out/jdbcversion_101.out 2>&1
checkout "JDBC Version 조회" "jdbcversion_101"

# 설정 생성(M001)
createcfg M001 "01" "PostgreSQL" "127.0.0.1" "5432" "hadoop" "" "hadoop" "st_m001" "public" >out/createcfg_101.out 2>&1
checkout "설정 생성(M001)" "createcfg_101"

# 메타 수집(M001)
jdbcmeta M001 >out/jdbcmeta_101.out 2>&1
checkout "메타 수집(M001)" "jdbcmeta_101"

# Rule 생성(M001)
buildrule M001 >out/buildrule_101.out 2>&1
checkout "Rule 생성(M001)" "buildrule_101"

# Rule 수정(M001)
updaterule M001 >out/updaterule_101.out 2>&1
checkout "Rule 수정(M001)" "updaterule_101"

# Rule 백업(M001)
backuprule M001 >out/backuprule_101.out 2>&1
checkout "Rule 백업(M001)" "backuprule_101"

# Rule 복구(M001)
restorerule M001 >out/restorerule_101.out 2>&1
checkout "Rule 복구(M001)" "restorerule_101"

cp cfg/IM-M001/* "$DATALAKE_COLLECTOR"/cfg/IM-M001/

# Rule 생성(M001)
buildrule M001 >out/buildrule_102.out 2>&1
checkout "Rule 생성(M001)" "buildrule_102"

# Rule 수정(M001)
updaterule M001 >out/updaterule_102.out 2>&1
checkout "Rule 수정(M001)" "updaterule_102"

# 설정 파일 생성(M001)
buildcfg M001 >out/buildcfg_106.out 2>&1
checkout "설정 파일 생성(M001)" "buildcfg_106"

# Rule 차이(0002)
diff "$DATALAKE_COLLECTOR"/cfg/IM-M001/DBMS-M001-T_RULE_META_0002.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/RULE-M001-T_RULE_META_0002.csv >out/t_rule_meta_0002_001.out 2>&1
checkout "Rule 차이(0002)" "t_rule_meta_0002_001"

# Rule 차이(0003)
diff "$DATALAKE_COLLECTOR"/cfg/IM-M001/DBMS-M001-T_RULE_META_0003.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/RULE-M001-T_RULE_META_0003.csv >out/t_rule_meta_0003_001.out 2>&1
checkout "Rule 차이(0003)" "t_rule_meta_0003_001"

# Rule 백업(M001)
backuprule M001 >out/backuprule_102.out 2>&1
checkout "Rule 백업(M001)" "backuprule_102"

mv "$DATALAKE_COLLECTOR"/cfg/IM-M001/AMEND-M001-T_RULE_META_0002.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/AMEND-M001-T_RULE_META_0002.bak
mv "$DATALAKE_COLLECTOR"/cfg/IM-M001/AMEND-M001-T_RULE_META_0003.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/AMEND-M001-T_RULE_META_0003.bak

# Rule 생성(M001)
buildrule M001 >out/buildrule_103.out 2>&1
checkout "Rule 생성(M001)" "buildrule_103"

# 설정 파일 생성(M001)
buildcfg M001 >out/buildcfg_107.out 2>&1
checkout "설정 파일 생성(M001)" "buildcfg_107"

# Rule 차이(0002)
diff "$DATALAKE_COLLECTOR"/cfg/IM-M001/DBMS-M001-T_RULE_META_0002.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/RULE-M001-T_RULE_META_0002.csv >out/t_rule_meta_0002_002.out 2>&1
checkout "Rule 차이(0002)" "t_rule_meta_0002_002"

# Rule 차이(0003)
diff "$DATALAKE_COLLECTOR"/cfg/IM-M001/DBMS-M001-T_RULE_META_0003.csv "$DATALAKE_COLLECTOR"/cfg/IM-M001/RULE-M001-T_RULE_META_0003.csv >out/t_rule_meta_0003_002.out 2>&1
checkout "Rule 차이(0003)" "t_rule_meta_0003_002"

# JDBC Type 조회
jdbctypes M001 "SELECT * FROM T_TYPES" >out/jdbctypes_101.out 2>&1
checkout "JDBC Type 조회" "jdbctypes_101"
