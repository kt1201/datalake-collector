DROP TABLE IF EXISTS T_RULE_META_0001;
DROP TABLE IF EXISTS T_RULE_META_0002;
DROP TABLE IF EXISTS T_RULE_META_0003;
DROP TABLE IF EXISTS T_BATCH_STATUS_0001;
DROP TABLE IF EXISTS T_BATCH_LOG_0001;
DROP TABLE IF EXISTS T_BATCH_LOG_0002;
DROP TABLE IF EXISTS T_BATCH_DATE_0001;
DROP TABLE IF EXISTS T_BATCH_RESULT_0001;
DROP TABLE IF EXISTS T_BATCH_RESULT_0002;
DROP TABLE IF EXISTS T_BATCH_RESULT_0003;

CREATE TABLE T_RULE_META_0001 (
    CNNC_MANAGE_NO               VARCHAR(256),
        -- 연결관리번호
    SYS_NM                       TEXT,
        -- 시스템명
    CNTC_MTHD_CODE               TEXT,
        -- 연계방식코드
    CNTC_MTHD_NM                 TEXT,
        -- 연계방식명
    DB_TY_CODE                   TEXT,
        -- DB유형코드
    DB_TY_NM                     TEXT,
        -- DB유형명
    DB_ACNT_NM                   TEXT,
        -- DB계정명
    HIVE_DB_NM                   TEXT,
        -- 하둡적재명
    DB_SERVICE_NM                TEXT,
        -- DB서비스명
    DB_1_SERVER_IP               TEXT,
        -- DB1서버IP
    DB_1_SERVER_PORT_NO          TEXT,
        -- DB1서버포트번호
    DB_2_SERVER_IP               TEXT,
        -- DB2서버IP
    DB_2_SERVER_PORT_NO          TEXT,
        -- DB2서버포트번호
    DB_USER_ID                   TEXT,
        -- DB사용자ID
    DB_USER_SECRET_NO            TEXT,
        -- DB사용자비밀번호
    REMOTE_SERVER_IP             TEXT,
        -- 원격서버IP
    REMOTE_SERVER_PORT_NO        TEXT,
        -- 원격서버포트번호
    REMOTE_SERVER_USER_ID        TEXT,
        -- 원격서버사용자ID
    REMOTE_SERVER_USER_SECRET_NO TEXT,
        -- 원격서버사용자비밀번호
    REMOTE_DRCTRY_NM             TEXT,
        -- 원격디렉토리명
    DATA_TY_CODE                 TEXT,
        -- 데이터유형코드
    API_DATA_AUTHKEY_NM          TEXT,
        -- OpenAPI데이터인증키명
    API_DATA_URL                 TEXT,
        -- OpenAPI데이터URL
    NTWK_SE_CODE                 TEXT,
        -- 망구분코드
    NTWK_SE_NM                   TEXT,
        -- 망구분명
    APPLC_SE_CODE                TEXT,
        -- 적용구분코드
    APPLC_SE_NM                  TEXT,
        -- 적용구분명
    REGIST_DE                    TEXT,
        -- 등록년월일
    REGIST_EMPL_NO               TEXT,
        -- 등록사원번호
    UPDT_DE                      TEXT,
        -- 수정년월일
    UPDT_EMPL_NO                 TEXT,
        -- 수정사원번호
    PRIMARY KEY ( CNNC_MANAGE_NO )
);

CREATE TABLE T_RULE_META_0002 (
    CNNC_MANAGE_NO      VARCHAR(256) NOT NULL,
        -- 연결관리번호
    TABLE_ENG_NM        VARCHAR(256) NOT NULL,
        -- 테이블영문명
    TABLE_KOREAN_NM     TEXT,
        -- 테이블한글명
    TABLE_DC            TEXT,
        -- 테이블설명
    BR_DC               TEXT,
        -- 업무규칙설명
    HASHTAG_CN          TEXT,
        -- 해시태그내용
    MNGR_NM             TEXT,
        -- 관리자명
    PART_COLS           TEXT,
        -- 파티션컬럼명
    WHERE_INFO_NM       TEXT,
        -- Where정보명
    INS_NUM_MAPPERS     TEXT,
        -- 매퍼수
    INS_SPLIT_BY_COL    TEXT,
        -- 분할기준칼럼
    APD_WHERE           TEXT,
        -- 변동적재기준조건
    APD_CHK_COL         TEXT,
        -- 변동적재기준칼럼명
    HIVE_TABLE_NM       TEXT,
        -- 하이브적재테이블영문명
    BIGDATA_GTRN_AT     TEXT,
        -- 빅데이터수집여부
    SCHDUL_APPLC_AT     TEXT,
        -- 스케쥴적용여부
    GTHNLDN_MTH_CODE    TEXT,
        -- 수집적재방법코드
    GTHNLDN_MTH_NM      TEXT,
        -- 수집적재방법명
    REGIST_DE           TEXT,
        -- 등록년월일
    REGIST_EMPL_NO      TEXT,
        -- 등록사원번호
    UPDT_DE             TEXT,
        -- 수정년월일
    UPDT_EMPL_NO        TEXT,
        -- 수정사원번호
    PRIMARY KEY ( CNNC_MANAGE_NO, TABLE_ENG_NM ),
    FOREIGN KEY ( CNNC_MANAGE_NO ) REFERENCES T_RULE_META_0001 ( CNNC_MANAGE_NO )
);

CREATE TABLE T_RULE_META_0003 (
    CNNC_MANAGE_NO           VARCHAR(256) NOT NULL,
        -- 연결관리번호
    TABLE_ENG_NM             VARCHAR(256) NOT NULL,
        -- 테이블영문명
    DB_TABLE_ATRB_SN         INTEGER,
        -- DB테이블속성일련번호
    TABLE_ATRB_ENG_NM        VARCHAR(512) NOT NULL,
        -- 테이블속성영문명
    TABLE_KOREAN_ATRB_NM     TEXT,
        -- 테이블한글속성명
    TABLE_ATRB_EXPR          TEXT,
        -- 테이블속성변환식
    TABLE_ATRB_DC            TEXT,
        -- 테이블속성설명
    DSTNG_TRGET_AT           TEXT,
        -- 비식별대상여부
    TABLE_ATRB_TY_NM         TEXT,
        -- 테이블속성타입명
    TABLE_ATRB_LT_VALUE      TEXT,
        -- 테이블속성길이값
    HIVE_COL_NM              TEXT,
        -- Hive속성명문명
    HIVE_ATRB_TY_NM          TEXT,
        -- Hive속성타입명
    TABLE_ATRB_NULL_POSBL_AT TEXT,
        -- 테이블속성NULL가능여부
    TABLE_ATRB_PK_AT         TEXT,
        -- 테이블속성PK여부
    REGIST_DE                TEXT,
        -- 등록년월일
    REGIST_EMPL_NO           TEXT,
        -- 등록사원번호
    UPDT_DE                  TEXT,
        -- 수정년월일
    UPDT_EMPL_NO             TEXT,
        -- 수정사원번호
    PRIMARY KEY ( CNNC_MANAGE_NO, TABLE_ENG_NM, TABLE_ATRB_ENG_NM ),
    FOREIGN KEY ( CNNC_MANAGE_NO, TABLE_ENG_NM ) REFERENCES T_RULE_META_0002 ( CNNC_MANAGE_NO, TABLE_ENG_NM )
);

CREATE TABLE T_BATCH_STATUS_0001 (
    PLANNED_DT           VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE       VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM         TEXT,
        -- 연계 방식명
    EXEC_NO              INTEGER,
        -- 배치 수행 회차
    STATUS               TEXT,
        -- 배치 수행 상태 (WAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT      TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT        TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    LAST_EXEC_NO         INTEGER,
        -- 최종 배치 수행 회차
    LAST_STATUS          TEXT,
        -- 최종 배치 수행 상태 (WAITING/EXECUTING/SUCCESS/FAILURE)
    LAST_ACTUAL_BEGIN_DT TEXT,
        -- 최종 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    LAST_ACTUAL_END_DT   TEXT,
        -- 최종 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE )
);

CREATE TABLE T_BATCH_LOG_0001 (
    PLANNED_DT           VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE       VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM         TEXT,
        -- 연계 방식명
    EXEC_NO              INTEGER,
        -- 배치 수행 회차
    CNNC_MANAGE_NO       VARCHAR(256),
        -- 연결 관리 번호
    STATUS               TEXT,
        -- 배치 수행 상태 (WAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT      TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT        TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    STDOUT               TEXT,
        -- 배치 실행 결과
    STDERR               TEXT,
        -- 배치 오류 결과
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE, EXEC_NO, CNNC_MANAGE_NO )
);

CREATE TABLE T_BATCH_LOG_0002 (
    PLANNED_DT           VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE       VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM         TEXT,
        -- 연계 방식명
    EXEC_NO              INTEGER,
        -- 배치 수행 회차
    CNNC_MANAGE_NO       VARCHAR(256),
        -- 연결 관리 번호
    TABLE_ENG_NM         VARCHAR(256),
        -- 테이블 영문명
    STATUS               TEXT,
        -- 배치 수행 상태 (WAITING/SKIPWAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT      TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT        TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    STDOUT               TEXT,
        -- 배치 실행 결과 로그
    STDERR               TEXT,
        -- 배치 오류 결과 로그
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE, EXEC_NO, CNNC_MANAGE_NO, TABLE_ENG_NM )
);

CREATE TABLE T_BATCH_DATE_0001 (
    PLANNED_DT VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    FLAG       INTEGER,
    PRIMARY KEY ( PLANNED_DT )
);

CREATE TABLE T_BATCH_RESULT_0001 (
    PLANNED_DT                    VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE                VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM                  TEXT,
        -- 연계 방식명
    STATUS                        TEXT,
        -- 배치 수행 상태 (WAITING/SKIPWAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT               TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT                 TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    ELAPSED                       BIGINT,
        -- 배치 소요 시간 (초)
    RECORDS                       BIGINT,
        -- 일배치 수집 레코드 수 
    ACCUMULATED_RECORDS           BIGINT,
        -- 테이블 전체 레코드 수
    INCREASED_ACCUMULATED_RECORDS BIGINT,
        -- 테이블 전체 레코드 증감수
    VERIFY                        TEXT,
        -- 수집/적재 검증 결과
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE )
);

CREATE TABLE T_BATCH_RESULT_0002 (
    PLANNED_DT                    VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE                VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM                  TEXT,
        -- 연계 방식명
    CNNC_MANAGE_NO                VARCHAR(256),
        -- 연결 관리 번호
    SYS_NM                        TEXT,
        -- 시스템명
    STATUS                        TEXT,
        -- 배치 수행 상태 (WAITING/SKIPWAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT               TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT                 TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    ELAPSED                       BIGINT,
        -- 배치 소요 시간 (초)
    RECORDS                       BIGINT,
        -- 일배치 수집 레코드 수 
    ACCUMULATED_RECORDS           BIGINT,
        -- 테이블 전체 레코드 수
    INCREASED_ACCUMULATED_RECORDS BIGINT,
        -- 테이블 전체 레코드 증감수
    VERIFY                        TEXT,
        -- 수집/적재 검증 결과
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE, CNNC_MANAGE_NO )
);

CREATE TABLE T_BATCH_RESULT_0003 (
    PLANNED_DT                    VARCHAR(50),
        -- 배치 계획 수행 일시 (YYYYMMDDHHMISS)
    CNTC_MTHD_CODE                VARCHAR(50)   NOT NULL,
        -- 연계 방식 코드
    CNTC_MTHD_NM                  TEXT,
        -- 연계 방식명
    CNNC_MANAGE_NO                VARCHAR(256),
        -- 연결 관리 번호
    SYS_NM                        TEXT,
        -- 시스템명
    TABLE_ENG_NM                  VARCHAR(256),
        -- 테이블 영문명
    TABLE_KOREAN_NM               TEXT,
        -- 테이블 한글명
    PART_COL_NM                   TEXT,
        -- 배치 수행 일시 파티션 컬럼명
    STATUS                        TEXT,
        -- 배치 수행 상태 (WAITING/SKIPWAITING/EXECUTING/SUCCESS/FAILURE)
    ACTUAL_BEGIN_DT               TEXT,
        -- 배치 실제 시작 일시 (YYYYMMDDHHMISS)
    ACTUAL_END_DT                 TEXT,
        -- 배치 실제 종료 일시 (YYYYMMDDHHMISS)
    ELAPSED                       BIGINT,
        -- 배치 소요 시간 (초)
    RECORDS                       BIGINT,
        -- 일배치 수집 레코드 수 
    ACCUMULATED_RECORDS           BIGINT,
        -- 테이블 전체 레코드 수
    INCREASED_ACCUMULATED_RECORDS BIGINT,
        -- 테이블 전체 레코드 증감수
    VERIFY                        TEXT,
        -- 수집/적재 검증 결과
    STDOUT                        TEXT,
        -- 배치 실행 결과 로그
    STDERR                        TEXT,
        -- 배치 오류 결과 로그
    PRIMARY KEY ( PLANNED_DT, CNTC_MTHD_CODE, CNNC_MANAGE_NO, TABLE_ENG_NM )
);
