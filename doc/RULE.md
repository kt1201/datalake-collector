# Rule 데이터베이스 구성 가이드

이 문서에서는 DBMS의 테이블 복제와 OpenAPI 수집을 위한 Rule 데이터베이스의 구성 방법 및 수집적재방법에 따른 수집/적재 절차를 기술한다.

> 본 문서에서 사용된 SVG 파일들은 GitLab의 결함으로 GitLab 보여지지 않을 수 있다. Clone 후에 VSCode의 Preview로 SVG파일의 그림을 정상적으로 볼 수 있다.<br>
> 본 문서에서 사용된 SequenceDiagram들은 [Markdown Preview Mermaid Support](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid)이 설치되어 있어야 화면상에 렌더링되어 출력된다.

<br>

## 목차

* [Rule 데이터베이스 관리](#Rule-데이터베이스-관리)
* [수집적재방법에 따른 처리 절차](#`수집적재방법`에-따른-처리-절차)
* [부록 - 수작업에 의한 메타정보 추출](#부록---수작업에-의한-메타정보-추출)

<br>

## Rule 데이터베이스 관리

Rule 데이터베이스 구성의 절차는 아래와 같다.

![Rule Build](img/Rule-Build.drawio.png)

각 단계별 구체적 진행 방식을 아래에 각기 별도의 단락으로 기술한다.

<br>

### 새로운 연결관리번호의 생성

`createcfg` 명령으로 새로운 `연결관리번호`의 설정파일을 생성할 수 있다.<br>

`연결관리번호`의 생성에 필요한 인자는 아래와 같다.

| 순번 | 인자명 | 필수·선택 | 설명 |
|:-:|---|:-:|---|
| 1 | 연결관리번호 | 필수 | 사용자가 부여한 고유 코드 |
| 2 | 수집유형코드 | 필수 | 설정 파일에 정의된 수집유형코드 중 선택 |
| 3 | DBMS유형 | 필수 | 설정 파일에 기술된 DBMS유형 |
| 4 | 서버IP | 필수 | DBMS 서버의 접속 IP |
| 5 | 서버포트 | 필수 | DBMS 서버의 접속 포트 |
| 6 | 사용자ID | 필수 | DBMS 서버의 접속 사용자명 |
| 7 | 비밀번호 | 필수 | DBMS 서버의 접속 비밀번호 |
| 8 | 서비스명 | 필수 | DBMS 서버의 접속 서비스명 혹은 데이터베이스명 등 |
| 9 | HIVE데이터베이스명 | 필수 | Hive 서버의 데이터베이스명 |
| 10 | 스키마명 | 선택 | DBMS 서버의 수집 대상 스키마명 |

> 설정 파일은 "cfg/IM-`연결관리번호`/SYS-`연결관리번호`.json`으로 생성된다.<br>
> Hive의 데이터베이스는 미리 생성해 두어야 한다.

호출 예제는 아래와 같다.

~~~bash
$ createcfg dbms_001 01 PostgreSQL10 localhost 5432 hadoop "" hadoop st_dbms_001
$ createcfg api_001 02 SQLite3 "" "" "" "" ":memory:" st_api_001
~~~

> OpenAPI 수집의 경우는 아래와 같이 호출하여 SQLite를 작업용 임시 DBMS로 사용할 수 있다.<br>
> `createcfg 연결관리번호 02 SQLite3 "" "" "" "" ":memory:" HIVE데이터베이스명`<br>
> `:memory:` 부분을 `공백`으로 두면 임시 파일에 저장하여 메모리 사용량은 감소하지만 속도는 하락할 수 있다.<br>
> 수집된 데이터의 저장을 Hive가 아닌 DBMS에 하고 싶을 경우 접속 대상 DBMS의 정보를 입력하여 수집/적재 시 대상 DBMS의 연결을 획득할 수 있다.<br>
> Hive의 데이터베이스는 미리 생성해 두어야 한다.

<br>

### 현재의 Rule 데이터베이스의 백업

`backuprule` 명령으로 `연결관리번호`의 현재의 Rule 데이터베이스를 백업할 수 있다.

백업된 내용은 아래의 파일에 생성된다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0001.csv | 백업된 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0002.csv | 백업된 `DB테이블정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0003.csv | 백업된 `DB속성정보` |

호출 예제는 아래와 같다.

~~~bash
$ backuprule dbms_001
시스템을 찾을 수 없음: dbms_001
Rule 백업 성공: dbms_001
$ backuprule api_001
시스템을 찾을 수 없음: api_001
Rule 백업 성공: api_001
~~~

<br>

### DBMS의 메타정보 추출

`jdbcmeta` 명령으로 `연결관리번호`의 수집 대상 DBMS의 Table들의 Meta 정보를 수집할 수 있다.<br>

백업된 내용은 아래의 파일에 생성된다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/META-`연결관리번호`-DATABASE.json | 수집된 데이터베이스의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-TABLES.csv | 수집된 테이블의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-COLUMNS.csv | 수집된 컬럼의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-PRIMARYKEYS.csv | 수집된 Primary Key의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-INDICES.csv | 수집된 인덱스의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-EXPORTEDKEYS.csv | 수집된 Foreign Key의 정보 |
| cfg/IM-`연결관리번호`/META-`연결관리번호`-IMPORTEDKEYS.csv | 수집된 Foreign Key의 정보 |

호출 예제는 아래와 같다.

~~~bash
$ jdbcmeta dbms_001
Schemas: [[None, 'information_schema'], [None, 'pg_catalog'], [None, 'public']]
메타 추출 성공: dbms_001
~~~

<br>

### Rule 수정 규칙 작성

`새로운 Rule 데이터베이스의 작성`에 앞서 Rule 데이터베이스 수정을 위한 파일들을 작성한다.


| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/AMEND-`연결관리번호`-T_RULE_META_0002.csv | 수정할 `DB테이블정보` |
| cfg/IM-`연결관리번호`/AMEND-`연결관리번호`-T_RULE_META_0003.csv | 수정할 `DB속성정보` |
| cfg/IM-`연결관리번호`/DENY-`연결관리번호`-T_RULE_META_0002.csv | 제외할 `DB테이블정보` |
| cfg/IM-`연결관리번호`/DENY-`연결관리번호`-T_RULE_META_0003.csv | 제외할 `DB속성정보` |

> 이러한 파일들에 필요한 내용들은 설계 문서에서 추출하거나, 메타 관리 소프트웨어등에서 추출할 수 있다.

각 파일들의 작성 예제는 아래와 같다.

##### 수정할 `DB테이블정보`
~~~
"TABLE_ENG_NM","TABLE_KOREAN_NM","BIGDATA_GTRN_AT"
"t_banner_link_mapping",,"Y"
"t_binary",,"Y"
"t_pnck_theme",,"Y"
"t_types",,"Y"
~~~
> "TABLE_ENG_NM" 이외의 컬럼은 테이블에 존재하는 컬럼에 한하여 필요에 따라 추가할 수 있다.

##### 수정할 `DB속성정보`
~~~
"TABLE_ENG_NM","TABLE_ATRB_ENG_NM","TABLE_KOREAN_ATRB_NM","TABLE_ATRB_EXPR","DSTNG_TRGET_AT","HIVE_ATRB_TY_NM"
"t_noty_recv","noty_id","알림ID",,,
"t_noty_recv","recv_id","받는사용자ID",,,
"t_noty_recv","recv_dt","받은날짜",,,
"t_noty_recv","view_yn","읽음유무",,,
"t_types","t_double",,"t_double::text",,"STRING"
"t_types","t_money",,"t_money::text",,"STRING"
"t_types","t_real",,"t_real::text",,"STRING"
"t_types","t_time",,"t_time::text",,"STRING"
~~~
> "TABLE_ENG_NM", "TABLE_ATRB_ENG_NM" 이외의 컬럼은 테이블에 존재하는 컬럼에 한하여 필요에 따라 추가할 수 있다.

##### 제외할 `DB테이블정보`
~~~
"TABLE_ENG_NM"
"getbeachlocationlist"
~~~

##### 제외할 `DB속성정보`
~~~
"TABLE_ENG_NM","TABLE_ATRB_ENG_NM"
"t_noty","send_dt"
~~~

<br>

### 새로운 Rule 데이터베이스의 작성

`buildrule` 명령으로 `연결관리번호`의 새로운 Rule 파일을 생성할 수 있다.

다음의 순서에 따라 백업된 CSV 파일과 수집된 메타 정보로 새로운 Rule을 CSV 파일로 작성한다.

* 수집된 META를 기준으로 새로운 Rule을 구성한다.
* 백업된 Rule이 있으면 수집된 META에 없는 정보를 새로운 Rule에 보충한다.<br>하지만, 한글주석과 데이터형 정보는 수정하지 않는다.
* 수정목록(AMEND)파일의 내용을 새로운 Rule에 덮어쓴다.
* 제외목록(DENY)파일의 내용을 새로운 Rule에서 제거한다.
* 새로운 Rule을 파일에 기록한다.

생성된 내용은 아래의 파일에 생성된다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0001.csv | 새로운 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0002.csv | 새로운 `DB테이블정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0003.csv | 새로운 `DB속성정보` |

> 생성된 파일은 Rule 데이터베이스의 반영에 앞서서 최종적으로 `수작업`에 의한 수정을 할 수 있다.<br>
> OpenAPI의 경우는 이 단계에서 `수작업`에 의해서 Rule을 구성하거나, 모든 내용을 `AMEND` 파일로 작성하여 처리한다.

호출 예제는 아래와 같다.

~~~bash
$ buildrule dbms_001
Rule 생성 성공: dbms_001
$ buildrule api_001
Rule 생성 성공: api_001
~~~

<br>

### 새로운 Rule 데이터베이스의 반영

`updaterule` 명령으로 `연결관리번호`의 새롭게 작성된 파일을 Rule 데이터베이스에 반영할 수 있다.

반영될 파일의 목록은 아래와 같다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0001.csv | 새로운 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0002.csv | 새로운 `DB테이블정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0003.csv | 새로운 `DB속성정보` |

호출 예제는 아래와 같다.

~~~bash
$ updaterule dbms_001
Rule 수정 성공: dbms_001
$ updaterule api_001
Rule 수정 성공: api_001
~~~

<br>

### 변경된 설정 파일의 작성

`buildcfg` 명령으로 `연결관리번호`의 새로운 설정 파일을 생성할 수 있다.

생성될 파일의 목록은 아래와 같다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/SYS-`연결관리번호`.json | `연결관리번호` 설정파일 |
| cfg/IM-`연결관리번호`/TAB-`연결관리번호`-`테이블영문명`.json | `연결관리번호`의 테이블별 설정파일 |

호출 예제는 아래와 같다.

~~~bash
$ buildcfg dbms_001
설정 파일 생성 성공: public.getbeachlocationlist
설정 파일 생성 성공: public.t_banner_link_mapping
설정 파일 생성 성공: public.t_binary
설정 파일 생성 성공: public.t_bookmark
설정 파일 생성 성공: public.t_code_mgmt
설정 파일 생성 성공: public.t_eplog
설정 파일 생성 성공: public.t_groups
설정 파일 생성 성공: public.t_link
설정 파일 생성 성공: public.t_link_user
설정 파일 생성 성공: public.t_noty
설정 파일 생성 성공: public.t_noty_recv
설정 파일 생성 성공: public.t_pnck_page
설정 파일 생성 성공: public.t_pnck_page_portlet_mapping
설정 파일 생성 성공: public.t_pnck_page_role
설정 파일 생성 성공: public.t_pnck_role
설정 파일 생성 성공: public.t_pnck_role_audit_log
설정 파일 생성 성공: public.t_pnck_theme
데이터형을 찾을 수 없음: MONEY
설정 파일 생성 성공: public.t_types
~~~

<br>

### 기존의 Rule 데이터베이스로 복원

`restorerule` 명령으로 `연결관리번호`의 백업된 파일을 Rule 데이터베이스에 복원할 수 있다.

복원될 파일의 목록은 아래와 같다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0001.csv | 백업된 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0002.csv | 백업된 `DB테이블정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0003.csv | 백업된 `DB속성정보` |

호출 예제는 아래와 같다.

~~~bash
$ restorerule dbms_001
Rule 복구 성공: dbms_001
$ restorerule api_001
Rule 복구 성공: api_001
~~~

<br>

### 수정된 메타정보의 추출 및 적용

수십개의 복수의 DBMS에 접속하여 수천개의 테이블의 정보를 수집하는 상황에서 수집 대상 시스템들은 수시로 개선되며 변경되며, 이에 따라 테이블들은 추가/삭제 되며 변경된다.<br>
이러한 변화에 따라가기 위해서 모든 시스템의 담당자들과 긴밀한 협의하여 Rule 데이터베이스의 변경을 관리하는 것은 어려운 일이다.<br>
다음의 절차에 따라 수집 대상 시스템들의 메타의 변경을 빠르게 감지하고 손쉽게 수정할 수 있다.

* `backuprule` 명령으로 Rule 데이터베이스를 파일에 백업한다.
* `jdbcmeta` 명령으로 수집 대상 DBMS의 테이블 정보를 다시 수집한다.
* `buildrule` 명령으로 새로운 Rule 파일을 생성한다.
* 백업된 Rule 파일과 새로운 Rule 파일을 `diff`로 비교하여 추가/삭제 혹은 변경된 테이블과 컬럼을 검출한다.
* 새로운 Rule 파일에 추가 혹은 변경된 테이블과 컬럼들의 정보를 적합하게 수정한다.
* `updaterule` 명령으로 새로운 Rule 파일을 Rule 데이터베이스에 반영한다.
* 필요한 경우 변경된 테이블에 대해서 Hive 데이터베이스에서 적절한 조치를 취한다.
* `buildcfg` 명령으로 새로운 설정파일을 생성한다.

다음의 파일들을 비교에 사용할 수 있다.

| 파일명 | 설명 |
|---|---|
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0001.csv | 백업된 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0002.csv | 백업된 `DB테이블정보` |
| cfg/IM-`연결관리번호`/DBMS-`연결관리번호`-T_RULE_META_0003.csv | 백업된 `DB속성정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0001.csv | 새로운 `수집대상연결정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0002.csv | 새로운 `DB테이블정보` |
| cfg/IM-`연결관리번호`/RULE-`연결관리번호`-T_RULE_META_0003.csv | 새로운 `DB속성정보` |

<br>

## `수집적재방법`에 따른 처리 절차

Datalake Collector에서는 `수집적재방법`에 따라 그에 해당하는 수집적재모듈이 호출되며 각 모듈의 수행절차는 각각 아래의 단락에서 설명한다.

<br>

### 000 OpenAPI 수집

코드 `000`은 OpenAPI 수집 혹은 그외의 다양한 방식의 수집을 위해서 사용될 수 있으며, `src/load000.py`에 의해서 처리된다.

상세한 처리의 흐름은 다음과 같다.

~~~mermaid
sequenceDiagram
    participant Handler(Python)
    participant Collector(Python)
    participant DataLake(Java)
    participant OpenAPI Provider
    participant Hive
    participant HDFS
    participant FileSystem
    Handler(Python)->>Collector(Python): 실행
    loop 수집
        Collector(Python)->>OpenAPI Provider: Request
        OpenAPI Provider->>Collector(Python): Response
    end
    Collector(Python)->>FileSystem: 수집 결과 CSV 파일에 저장
    Collector(Python)->>Hive: CREATE TABLE IF NOT EXIST ...
    Collector(Python)->>Hive: ALTER TABLE ... DROP PARTITION ...
    Collector(Python)->>Hive: ALTER TABLE ... ADD PARTITION ...
    Collector(Python)->>DataLake(Java): 실행
    loop 적재
        FileSystem->>DataLake(Java): 레코드 조회
        DataLake(Java)->>HDFS: 레코드 추가
    end
    DataLake(Java)->>Collector(Python): 종료
    Collector(Python)->>Hive: ANALYZE TABLE ... COMPUTE STATISTICS
    Collector(Python)->>Hive: ALTER TABLE ... DROP PARTITION ... (기존 파티션)
    Collector(Python)->>DataLake(Java): 실행
    loop 검증 데이터 추출
        Hive->>DataLake(Java): 레코드 조회
        DataLake(Java)->>FileSystem: 적재 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Collector(Python): 종료
    Collector(Python)->>FileSystem: 수집 결과 CSV 파일의 MD5 해시값 계산
    Collector(Python)->>FileSystem: 적재 결과 CSV 파일의 MD5 해시값 계산
    Collector(Python)->>Handler(Python): 종료
~~~

<br>

### 001 테이블 재생성 후 수집/적재

코드 `001`은 DBMS의 테이블의 전체 레코드의 복제에 사용될 수 있으며, `src/load001.py`에 의해서 처리된다.

테이블이 항상 재생성되어 Rule 데이터베이스의 변경에도 Hive의 테이블에 대한 수정 작업을 하지 않는 장점이 있다.

상세한 처리의 흐름은 다음과 같다.

~~~mermaid
sequenceDiagram
    participant Handler(Python)
    participant DataLake(Java)
    participant RDBMS
    participant Hive
    participant HDFS
    participant FileSystem
    Handler(Python)->>Hive: DROP TABLE IF EXIST ...
    Handler(Python)->>Hive: CREATE TABLE IF NOT EXIST ...
    Handler(Python)->>DataLake(Java): 실행
    loop 수집/적재
        RDBMS->>DataLake(Java): 레코드 조회
        DataLake(Java)->>HDFS: 레코드 추가
        DataLake(Java)->>FileSystem: 수집 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>Hive: ANALYZE TABLE ... COMPUTE STATISTICS
    Handler(Python)->>DataLake(Java): 실행
    loop 검증 데이터 추출
        Hive->>DataLake(Java): 레코드 조회
        DataLake(Java)->>FileSystem: 적재 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>FileSystem: 수집 결과 CSV 파일의 MD5 해시값 계산
    Handler(Python)->>FileSystem: 적재 결과 CSV 파일의 MD5 해시값 계산
~~~

<br>

### 002 테이블 초기화 후 수집/적재

코드 `002`은 DBMS의 테이블의 전체 레코드의 복제에 사용될 수 있으며, `src/load002.py`에 의해서 처리된다.

테이블을 `Truncate Table`로 초기화 후에 테이블 수집을 한다.

~~~mermaid
sequenceDiagram
    participant Handler(Python)
    participant DataLake(Java)
    participant RDBMS
    participant Hive
    participant HDFS
    participant FileSystem
    Handler(Python)->>Hive: CREATE TABLE IF NOT EXIST ...
    Handler(Python)->>Hive: TRUNCATE TABLE ...
    Handler(Python)->>DataLake(Java): 실행
    loop 수집/적재
        RDBMS->>DataLake(Java): 레코드 조회
        DataLake(Java)->>HDFS: 레코드 추가
        DataLake(Java)->>FileSystem: 수집 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>Hive: ANALYZE TABLE ... COMPUTE STATISTICS
    Handler(Python)->>DataLake(Java): 실행
    loop 검증 데이터 추출
        Hive->>DataLake(Java): 레코드 조회
        DataLake(Java)->>FileSystem: 적재 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>FileSystem: 수집 결과 CSV 파일의 MD5 해시값 계산
    Handler(Python)->>FileSystem: 적재 결과 CSV 파일의 MD5 해시값 계산
~~~

상세한 처리의 흐름은 다음과 같다.

<br>

### 003 파티션 재생성 후 수집/적재

코드 `003`은 DBMS의 테이블의 전체 레코드의 복제에 사용될 수 있으며, `src/load003.py`에 의해서 처리된다.

테이블에 새로운 파티션을 생성하고 적재한 후, 기존의 파티션을 삭제하여 수집/적재 작업이 실패하더라도 마지만 성공한 데이터를 회복할 수 있다.

대신 파티션 조건 없이 조회하면 일시적으로 레코드가 중복되어 조회될 수 있다.

상세한 처리의 흐름은 다음과 같다.

~~~mermaid
sequenceDiagram
    participant Handler(Python)
    participant DataLake(Java)
    participant RDBMS
    participant Hive
    participant HDFS
    participant FileSystem
    Handler(Python)->>Hive: CREATE TABLE IF NOT EXIST ...
    Handler(Python)->>Hive: ALTER TABLE ... DROP PARTITION ...
    Handler(Python)->>Hive: ALTER TABLE ... ADD PARTITION ...
    Handler(Python)->>DataLake(Java): 실행
    loop 수집/적재
        RDBMS->>DataLake(Java): 레코드 조회
        DataLake(Java)->>HDFS: 레코드 추가
        DataLake(Java)->>FileSystem: 수집 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>Hive: ANALYZE TABLE ... COMPUTE STATISTICS
    Handler(Python)->>DataLake(Java): 실행
    loop 검증 데이터 추출
        Hive->>DataLake(Java): 레코드 조회
        DataLake(Java)->>FileSystem: 적재 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>FileSystem: 수집 결과 CSV 파일의 MD5 해시값 계산
    Handler(Python)->>FileSystem: 적재 결과 CSV 파일의 MD5 해시값 계산
    Handler(Python)->>Hive: ALTER TABLE ... DROP PARTITION ... (기존 파티션)
~~~

<br>

### 004 일별 테이블을 일별 파티션에 누적 수집/적재

코드 `004`은 레코드의 수가 많아 매일 새로운 테이블에 데이터가 적재되는 경우에 사용될 수 있으며, `src/load004.py`에 의해서 처리된다.

일별로 파티션을 작성하고 일별 테이블의 레코들들은 파티션에 복제한다.

상세한 처리의 흐름은 다음과 같다.

~~~mermaid
sequenceDiagram
    participant Handler(Python)
    participant DataLake(Java)
    participant RDBMS
    participant Hive
    participant HDFS
    participant FileSystem
    Note over Handler(Python): 테이블 목록 수집
    Note over Handler(Python): 파티션 목록 수집
    loop 최근 지정 일수(Short) 내 테이블 무조건 수집 / 최근 지정 일수(Long) 내 테이블 일별 파티션이 없을 경우 수집
    Handler(Python)->>Hive: CREATE TABLE IF NOT EXIST ...
    Handler(Python)->>Hive: ALTER TABLE ... DROP PARTITION ...
    Handler(Python)->>Hive: ALTER TABLE ... ADD PARTITION ...
    Handler(Python)->>DataLake(Java): 실행
    loop 수집/적재
        RDBMS->>DataLake(Java): 레코드 조회
        DataLake(Java)->>HDFS: 레코드 추가
        DataLake(Java)->>FileSystem: 수집 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>Hive: ANALYZE TABLE ... COMPUTE STATISTICS
    Handler(Python)->>DataLake(Java): 실행
    loop 검증 데이터 추출
        Hive->>DataLake(Java): 레코드 조회
        DataLake(Java)->>FileSystem: 적재 결과 CSV 파일에 추가
    end
    DataLake(Java)->>Handler(Python): 종료
    Handler(Python)->>FileSystem: 수집 결과 CSV 파일의 MD5 해시값 계산
    Handler(Python)->>FileSystem: 적재 결과 CSV 파일의 MD5 해시값 계산
    end
~~~

> 이 수집/적재 방식은 DBMS와 Hive의 내부 정보를 조회하여 다양한 컨트롤을 수행할 수 있음으로 아래의 사용자 `수집적재방법`의 기본 포맷으로 활용할 수 있다.

<br>

### 1XXX 사용자 `수집적재방법`의 추가 절차

위의 `수집적재방법`들에 의해서 수집이 어려운 경우 유사성이 있는 개별 테이블 그룹별로 새로운 `수집적재방법`을 추가하여 처리할 수 있다.<br>
`src/load*.py` 파일들을 참조하여 복제하여 수정하는 것이 좋은 시작 방법이다.<br>
새롭게 추가된 파일은 `src/batchrun.py` 파일의 변수 `handlers`에 추가하여야 사용할 수 있다.

~~~python
handlers = {"000": load000, "00": load000, "0": load000,
            "001": load001, "01": load001, "1": load001,
            "002": load002, "02": load002, "2": load002,
            "003": load003, "03": load003, "3": load003,
            "004": load004, "04": load004, "4": load004}
~~~

<br>

### 위의 방법으로 도저히 데이터 수집이 불가능하다고 판단되는 경우

이 제품은 DBMS와 Hive의 테이블간의 1:1 복제에는 장점이 있으며, 매일 전체 테이블의 모든 레코드를 복제하여 문제가 해결될 수 있다면 더할 나위없이 적합하다.

단일 Thread의 처리에 있어서 100만 레코드는 1~2분, 1000만 레코드는 10~20분 사이에 완료되어 1000만 건 이하의 레코드를 가진 테이블들에 대해서는 특별한 경우가 아니면 위의 방식으로 처리할 수 있을 것이다.

하지만, 데이터의 양이 너무 많거나 추출 속도가 너무 느린 경우등은 이 제품만을 고집하지 않고 기존의 다른 방식을 활용하여 처리하는 것이 현명할 수도 있다.

이러한 예외의 경우가 발생하더라도 많은 수의 테이블들을 이 제품으로 수집/적재하여 처리하고, 소수의 테이블만을 별도의 방식으로 처리하는 것으로 인력 투입 비용을 줄일 수 있는 효과는 있을 것 이다.

<br>

## 부록 - 수작업에 의한 메타정보 추출

`Datalake Collector`는 JDBC 접속이 가능할 경우 JDBC의 표준 기능으로 테이블 및 컬럼의 정보를 수집할 수 있어, 위의 절차에 따라 Rule 데이터베이스를 구축할 수 있다.

하지만, 분석 단계에서 수집 서버의 구성이 불가능하거나 JDBC 접속이 불가능하여 이러한 기능을 사용할 수 없을 수 있다.<br>
또, 권한 상의 문제로 수집이 되지 않는 경우도 있다.

이러한 경우에는 각 데이터베이스에 직접 접근하여 Query를 사용하여 기초 자료를 수집할 수 있으며, 상황에 따라서는 관리자 권한으로 접속할 필요도 있다.

아래에 몇 종류의 DBMS에서 Meta 정보를 수집하는 Query를 첨부하니, 적당히 수정하여 사용할 수 있다.

<br>

### Oracle의 메타정보 추출

`Oracle`에서는 아래의 쿼리로 사용자가 접근할 수 있는 테이블과 컬럼의 정보를 확보할 수 있다.

~~~sql
SET LINESIZE 32000;
SET PAGESIZE 40000;
SET LONG 50000;

SELECT A.TABLE_NAME   AS TAB_NAME,
       A.TAB_CMT      AS TAB_COMMENT,
       A.COLUMN_NAME  AS COL_NAME,
       B.POS          AS COL_PK,
       A.COL_CMT      AS COL_COMMENT,
       A.DATA_TYPE    AS COL_DATA_TYPE,
       A.DATA_LEN     AS COL_LENGTH,
       A.NULLABLE     AS COL_NULLABLE,
       A.COLUMN_ID    AS COL_ORDER,
       A.DATA_DEFAULT AS COL_DEFAULT
  FROM ( SELECT S1.TABLE_NAME,
                S3.COMMENTS AS TAB_CMT,
                S1.COLUMN_NAME,
                S2.COMMENTS AS COL_CMT,
                S1.DATA_TYPE,
                DATA_PRECISION||','||DATA_SCALE||','||S1.DATA_LENGTH AS DATA_LEN,
                NULLABLE,
                COLUMN_ID,
                DATA_DEFAULT
           FROM USER_TAB_COLUMNS  S1,
                USER_COL_COMMENTS S2,
                USER_TAB_COMMENTS S3
          WHERE S1.TABLE_NAME  = S2.TABLE_NAME
            AND S1.COLUMN_NAME = S2.COLUMN_NAME
            AND S2.TABLE_NAME  = S3.TABLE_NAME ) A,        
       ( SELECT T1.TABLE_NAME, T2.COLUMN_NAME, 'PK'||POSITION AS POS
           FROM ( SELECT TABLE_NAME, CONSTRAINT_NAME  
                    FROM USER_CONSTRAINTS
                   WHERE CONSTRAINT_TYPE = 'P' )T1,
                ( SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, POSITION
                    FROM USER_CONS_COLUMNS ) T2
          WHERE T1.TABLE_NAME      = T2.TABLE_NAME
            AND T1.CONSTRAINT_NAME = T2.CONSTRAINT_NAME  ) B
 WHERE A.TABLE_NAME  = B.TABLE_NAME(+)
   AND A.COLUMN_NAME = B.COLUMN_NAME(+)    
 ORDER BY A.TABLE_NAME, A.COLUMN_ID;
 ~~~

상황에 따라서는 접근할 수 있지만 위의 쿼리로 메타 정보를 수집할 수 없을 경우 ALL_TABLES 기반으로 변경하여 수집을 시도할 수 있다.

~~~sql
SET LINESIZE 32000;
SET PAGESIZE 40000;
SET LONG 50000;

SELECT OWNER, COUNT(*) FROM ALL_TABLES GROUP BY OWNER;

SELECT A.TAB_OWNER       AS TAB_OWNER,
       A.TAB_NAME        AS TAB_NAME,
       A.TAB_COMMENT     AS TAB_COMMENT,
       A.COL_NAME        AS COL_NAME,
       B.COL_PK          AS COL_PK,
       A.COL_COMMENT     AS COL_COMMENT,
       A.COL_DATA_TYPE   AS COL_DATA_TYPE,
       A.COL_DATA_LENGTH AS COL_LENGTH,
       A.COL_NULLABLE    AS COL_NULLABLE,
       A.COL_ORDER       AS COL_ORDER,
       A.COL_DEFAULT     AS COL_DEFAULT
  FROM ( SELECT S1.OWNER        AS TAB_OWNER,
                S1.TABLE_NAME   AS TAB_NAME,
                S3.COMMENTS     AS TAB_COMMENT,
                S1.COLUMN_NAME  AS COL_NAME,
                S2.COMMENTS     AS COL_COMMENT,
                S1.DATA_TYPE    AS COL_DATA_TYPE,
                S1.DATA_PRECISION||','||S1.DATA_SCALE||','||S1.DATA_LENGTH
                                AS COL_DATA_LENGTH,
                S1.NULLABLE     AS COL_NULLABLE,
                S1.COLUMN_ID    AS COL_ORDER,
                S1.DATA_DEFAULT AS COL_DEFAULT
           FROM ALL_TAB_COLUMNS  S1,
                ALL_COL_COMMENTS S2,
                ALL_TAB_COMMENTS S3
          WHERE S1.OWNER       = 'SYSTEM'
            AND S1.OWNER       = S2.OWNER
            AND S1.TABLE_NAME  = S2.TABLE_NAME
            AND S1.COLUMN_NAME = S2.COLUMN_NAME
            AND S1.OWNER       = S3.OWNER
            AND S2.TABLE_NAME  = S3.TABLE_NAME ) A,        
       ( SELECT T1.OWNER          AS TAB_OWNER,
                T1.TABLE_NAME     AS TAB_NAME,
                T2.COLUMN_NAME    AS COL_NAME,
                'PK'||T2.POSITION AS COL_PK
           FROM ( SELECT OWNER,
                         TABLE_NAME,
                         CONSTRAINT_NAME
                    FROM ALL_CONSTRAINTS
                   WHERE CONSTRAINT_TYPE = 'P' ) T1,
                ( SELECT OWNER,
                         TABLE_NAME,
                         CONSTRAINT_NAME,
                         COLUMN_NAME,
                         POSITION
                    FROM ALL_CONS_COLUMNS ) T2
          WHERE T1.OWNER           = T2.OWNER
            AND T1.TABLE_NAME      = T2.TABLE_NAME
            AND T1.CONSTRAINT_NAME = T2.CONSTRAINT_NAME ) B
 WHERE A.TAB_OWNER = B.TAB_OWNER(+)
   AND A.TAB_NAME  = B.TAB_NAME(+)
   AND A.COL_NAME  = B.COL_NAME(+)
 ORDER BY A.TAB_OWNER, A.TAB_NAME, A.COL_ORDER;
 ~~~

> ALL_TABLES 기반의 쿼리의 경우 수행시간이 인내하기 어려울 정도로 길수도 있다. 이러한 경우 OWNER에 대한 조건을 추가하여 OWNER 별로 수집하여 수행시간을 단축시킬수 있다.

<br>

### Cubrid의 메타정보 추출

`Cubrid`에서는 아래의 쿼리로 테이블과 컬럼의 정보를 수집할 수 있다.

~~~sql
select t.owner_name    as TAB_OWNER,
       t.class_name    as TAB_NAME,
       t.comment       as TAB_COMMENT,
       c.attr_name     as COL_NAME,
       ( select 'PK'||key_order
           from db_index i, db_index_key k
          where i.class_name    = t.class_name
            and k.key_attr_name = c.attr_name
            and i.class_name    = k.class_name
            and i.index_name    = k.index_name
            and i.is_primary_key = 'YES' )
                       as COL_PK,
       c.comment       as COL_COMMENT,
       c.data_type     as COL_DATA_TYPE,
       c.prec||','||c.scale         
                       as COL_LENGTH,
       c.is_nullable   as COL_NULLABLE,
       c.def_order     as COL_ORDER,
       c.default_value as COL_DEFAULT
  from db_class t, db_attribute c
 where t.class_name = c.class_name
   and t.class_type = 'CLASS'
   and t.is_system_class <> 'YES'
 order by TAB_OWNER, TAB_NAME, COL_ORDER;
 ~~~

> 버전에 따라 comment를 지원하지 않아 해당 컬럼이 없는 경우가 있다. 이러한 경우 쿼리에서 해당 컬럼을 제외하고 수행한다.

<br>

### MySQL의 메타정보 추출

`MySQL`에서는 아래의 쿼리로 테이블과 컬럼의 정보를 수집할 수 있다.

~~~sql
select tc.table_schema             as TAB_SCHEMA,
       tc.table_name               as TAB_NAME,
       tc.table_type               as TAB_TYPE,
       tc.table_comment            as TAB_COMMENT,
       tc.column_name              as COL_NAME,
       s.pk                        as COL_PK,
       tc.column_comment           as COL_COMMENT,
       tc.data_type                as COL_DATA_TYPE,
       concat(ifnull(tc.numeric_precision,''),',',ifnull(tc.numeric_scale,''),',',ifnull(tc.numeric_precision,''))
                                   as COL_LENGTH,
       tc.is_nullable              as COL_NULLABLE,
       tc.ordinal_position         as COL_ORDER,
       tc.column_default           as COL_DEFAULT
  from ( select t.table_catalog,
                t.table_schema,
                t.table_name,
                t.table_type,
                t.table_comment,
                c.column_name,
                c.ordinal_position,
                c.column_default,
                c.is_nullable,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.column_comment
           from information_schema.TABLES     t,
                information_schema.COLUMNS    c
          where t.table_catalog = c.table_catalog
            and t.table_schema  = c.table_schema
            and t.table_name    = c.table_name ) tc
       left outer join
       ( select table_catalog,
                table_schema,
                table_name,
                column_name,
                concat('PK',seq_in_index) as pk
           from information_schema.STATISTICS
          where index_name = 'PRIMARY' ) s
       on     tc.table_catalog = s.table_catalog
          and tc.table_schema  = s.table_schema
          and tc.table_name    = s.table_name
          and tc.column_name   = s.column_name
 where tc.table_schema <> 'information_schema'
   and tc.table_schema <> 'performance_schema'
 order by TAB_SCHEMA, TAB_NAME, COL_ORDER;
 ~~~
