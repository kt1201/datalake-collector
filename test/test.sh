#!/bin/bash

# 이전 수행 결과 파일 삭제
rm -f data/*.db data/*.db-journal out/* ../tmp/*.source.csv  ../tmp/*.target.csv

# 테스트 설정 파일 복사
cp cfg/healthcheck.json "$DATALAKE_COLLECTOR"/cfg/healthcheck.json
rm -rf "$DATALAKE_COLLECTOR"/cfg/IM-* "$DATALAKE_COLLECTOR"/cfg/datalake.json

# 테스트 Rule Database 구성
sqlite3 data/ruledb.db <<EOF
.read "$DATALAKE_COLLECTOR/sql/schema.sql"
.mode csv
.import data/t_rule_meta_0001.csv t_rule_meta_0001
.import data/t_rule_meta_0002.csv t_rule_meta_0002
.import data/t_rule_meta_0003.csv t_rule_meta_0003
EOF

# 테스트 Rule Database 구성
psql -U hadoop hadoop <<EOF >/dev/null
\i data/source.sql
EOF

# 테스트 Rule Database 구성
sqlite3 data/target.db <<EOF
.read "data/target.sql"
EOF

# Hive 데이터베이스 생성
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_m001 cascade;"     2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_m001;"                     2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_kea_l001 cascade;" 2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_kea_l001;"                 2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_kea_r001 cascade;" 2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_kea_r001;"                 2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_api_001 cascade;"  2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_api_001;"                  2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_api_002 cascade;"  2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_api_002;"                  2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "drop database if exists st_api_003 cascade;"  2>/dev/null
beeline -u "jdbc:hive2://localhost:10000" -e "create database st_api_003;"                  2>/dev/null

# 헬스 체크 수행
"$DATALAKE_COLLECTOR/bin/healthcheck"

# 설정 파일 생성 테스트
./test1.sh

# 저장소 자동 생성 테스트
./test2.sh

# 수집/적재 테스트
./test3.sh

# OPENAPI 테스트
./test4.sh

# 배치 테스트
./test5.sh
