all:

test: all
	cd test; ./test.sh

package: datalake-collector.tgz

datalake-collector.tgz: README.md \
                        bin/healthcheck bin/buildcfg bin/createcfg \
                        bin/buildrule bin/backuprule bin/restorerule bin/updaterule \
                        bin/batchcheck bin/batchrun bin/batchresult \
                        bin/hiveanalyze \
                        bin/hivecreatetable bin/hivedroptable bin/hivetruncatetable \
                        bin/hivecreatepartition bin/hiveclearpartition \
                        bin/hivedroppartition bin/hivelistpartition \
                        bin/hive2csv bin/hive2jdbc bin/hivecount \
                        bin/hivequery bin/hiveshell \
                        bin/hiveverify \
                        bin/csv2jdbc bin/csv2seqfile \
                        bin/jdbc2csv bin/jdbc2jdbc bin/jdbc2seqfile \
                        bin/jdbccount bin/jdbcmeta bin/jdbcquery \
                        bin/jdbcshell bin/jdbctypes bin/jdbcversion \
                        bin/jdbctruncatetable \
                        cfg/classes.json cfg/datatypes.json \
                        cfg/healthcheck-postgres.json cfg/datalake-sqlite.json \
                        doc/BATCH.md doc/OPENAPI.md doc/PROXY.md doc/RULE.md \
                        doc/dat/sample.csv \
                        doc/dat/t_rule_meta_0001.csv doc/dat/t_rule_meta_0002.csv doc/dat/t_rule_meta_0003.csv \
                        doc/img/Batch-Process.drawio.png doc/img/Rule-Build.drawio.png \
                        doc/img/Batch-Process.drawio.svg doc/img/Rule-Build.drawio.svg \
                        doc/img/OpenAPI-MakeURL1.png doc/img/OpenAPI-MakeURL2.png \
                        doc/img/Proxy-Server-1.drawio.png doc/img/Proxy-Server-2.drawio.png \
                        doc/img/Proxy-Server-1.drawio.svg doc/img/Proxy-Server-2.drawio.svg \
                        doc/img/GitLab-Download.png \
                        doc/img/T_BATCH_LOG_0000.png doc/img/T_BATCH_RESULT_0000.png doc/img/T_RULE_META_0000.png \
                        lib/datalake-collector-1.0.2.jar \
                        lib/hive-jdbc-2.3.7-standalone.jar lib/hive-jdbc-3.1.2-standalone.jar \
                        lib/Cloudera-JDBC-Driver-for-Apache-Hive-Install-Guide.pdf \
                        lib/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf \
                        lib/HiveJDBC4.jar  lib/HiveJDBC41.jar lib/HiveJDBC42.jar \
                        lib/ImpalaJDBC4.jar lib/ImpalaJDBC41.jar lib/ImpalaJDBC42.jar \
                        lib/ojdbc6.jar lib/tibero6-jdbc.jar lib/mssql-jdbc-8.2.2.jre8.jar \
                        lib/JDBC-10.2-latest-cubrid.jar \
                        lib/mysql-connector-java-5.1.49.jar lib/postgresql-42.2.16.jar \
                        openapi/openapi.py openapi/template1.py openapi/template2.py openapi/template3.py \
                        python/pip-install.sh python/pip-uninstall.sh \
                        python/six-1.15.0-py2.py3-none-any.whl \
                        python/python_dateutil-2.8.1-py2.py3-none-any.whl \
                        python/xmltodict-0.12.0-py2.py3-none-any.whl \
                        sql/schema.sql \
                        src/healthcheck.py src/buildcfg.py src/createcfg.py \
                        src/buildrule.py src/backuprule.py src/restorerule.py src/updaterule.py \
                        src/batchcheck.py src/batchrun.py src/batchresult.py \
                        src/load000.py src/load001.py src/load002.py src/load003.py src/load004.py \
                        src/csv2jdbc.py src/csv2seqfile.py src/csvutil.py \
                        src/hiveanalyze.py \
                        src/hivecreatetable.py src/hivedroptable.py src/hivetruncatetable.py \
                        src/hivecreatepartition.py src/hiveclearpartition.py \
                        src/hivedroppartition.py src/hivelistpartition.py \
                        src/hive2csv.py src/hive2jdbc.py src/hivecount.py \
                        src/hivequery.py src/hiveshell.py \
                        src/hiveverify.py \
                        src/jdbc2json.py \
                        src/jdbcconf.py src/jdbcutil.py \
                        src/jdbc2csv.py src/jdbc2jdbc.py src/jdbc2seqfile.py \
                        src/jdbccount.py src/jdbcmeta.py src/jdbcquery.py \
                        src/jdbcshell.py src/jdbctypes.py src/jdbcversion.py \
                        src/jdbctruncatetable.py \
                        src/secret.py
	rm -rf package/datalake-collector
	mkdir package/datalake-collector
	mkdir package/datalake-collector/bin
	mkdir package/datalake-collector/cfg
	mkdir package/datalake-collector/doc
	mkdir package/datalake-collector/doc/img
	mkdir package/datalake-collector/doc/dat
	mkdir package/datalake-collector/lib
	mkdir package/datalake-collector/openapi
	mkdir package/datalake-collector/python
	mkdir package/datalake-collector/sql
	mkdir package/datalake-collector/src
	mkdir package/datalake-collector/tmp
	install -m 0644 README.md                                                  package/datalake-collector/
	install -m 0755 bin/healthcheck                                            package/datalake-collector/bin/
	install -m 0755 bin/buildcfg                                               package/datalake-collector/bin/
	install -m 0755 bin/createcfg                                              package/datalake-collector/bin/
	install -m 0755 bin/buildrule                                              package/datalake-collector/bin/
	install -m 0755 bin/backuprule                                             package/datalake-collector/bin/
	install -m 0755 bin/restorerule                                            package/datalake-collector/bin/
	install -m 0755 bin/updaterule                                             package/datalake-collector/bin/
	install -m 0755 bin/batchcheck                                             package/datalake-collector/bin/
	install -m 0755 bin/batchrun                                               package/datalake-collector/bin/
	install -m 0755 bin/batchresult                                            package/datalake-collector/bin/
	install -m 0755 bin/hiveanalyze                                            package/datalake-collector/bin/
	install -m 0755 bin/hivecreatetable                                        package/datalake-collector/bin/
	install -m 0755 bin/hivedroptable                                          package/datalake-collector/bin/
	install -m 0755 bin/hivetruncatetable                                      package/datalake-collector/bin/
	install -m 0755 bin/hivecreatepartition                                    package/datalake-collector/bin/
	install -m 0755 bin/hiveclearpartition                                     package/datalake-collector/bin/
	install -m 0755 bin/hivedroppartition                                      package/datalake-collector/bin/
	install -m 0755 bin/hivelistpartition                                      package/datalake-collector/bin/
	install -m 0755 bin/hive2csv                                               package/datalake-collector/bin/
	install -m 0755 bin/hive2jdbc                                              package/datalake-collector/bin/
	install -m 0755 bin/hivecount                                              package/datalake-collector/bin/
	install -m 0755 bin/hivequery                                              package/datalake-collector/bin/
	install -m 0755 bin/hiveshell                                              package/datalake-collector/bin/
	install -m 0755 bin/hiveverify                                             package/datalake-collector/bin/
	install -m 0755 bin/csv2jdbc                                               package/datalake-collector/bin/
	install -m 0755 bin/csv2seqfile                                            package/datalake-collector/bin/
	install -m 0755 bin/jdbc2csv                                               package/datalake-collector/bin/
	install -m 0755 bin/jdbc2jdbc                                              package/datalake-collector/bin/
	install -m 0755 bin/jdbc2seqfile                                           package/datalake-collector/bin/
	install -m 0755 bin/jdbccount                                              package/datalake-collector/bin/
	install -m 0755 bin/jdbcmeta                                               package/datalake-collector/bin/
	install -m 0755 bin/jdbcquery                                              package/datalake-collector/bin/
	install -m 0755 bin/jdbcshell                                              package/datalake-collector/bin/
	install -m 0755 bin/jdbctypes                                              package/datalake-collector/bin/
	install -m 0755 bin/jdbcversion                                            package/datalake-collector/bin/
	install -m 0755 bin/jdbctruncatetable                                      package/datalake-collector/bin/
	install -m 0644 cfg/classes.json                                           package/datalake-collector/cfg/
	install -m 0644 cfg/datatypes.json                                         package/datalake-collector/cfg/
	install -m 0644 cfg/healthcheck-postgres.json                              package/datalake-collector/cfg/healthcheck.sample.json
	install -m 0644 cfg/datalake-sqlite.json                                   package/datalake-collector/cfg/datalake.sample.json
	install -m 0644 doc/BATCH.md                                               package/datalake-collector/doc/
	install -m 0644 doc/OPENAPI.md                                             package/datalake-collector/doc/
	install -m 0644 doc/PROXY.md                                               package/datalake-collector/doc/
	install -m 0644 doc/RULE.md                                                package/datalake-collector/doc/
	install -m 0644 doc/dat/sample.csv                                         package/datalake-collector/doc/dat/
	install -m 0644 doc/dat/t_rule_meta_0001.csv                               package/datalake-collector/doc/dat/
	install -m 0644 doc/dat/t_rule_meta_0002.csv                               package/datalake-collector/doc/dat/
	install -m 0644 doc/dat/t_rule_meta_0003.csv                               package/datalake-collector/doc/dat/
	install -m 0644 doc/img/Batch-Process.drawio.svg                           package/datalake-collector/doc/img/
	install -m 0644 doc/img/Rule-Build.drawio.svg                              package/datalake-collector/doc/img/
	install -m 0644 doc/img/OpenAPI-MakeURL1.png                               package/datalake-collector/doc/img/
	install -m 0644 doc/img/OpenAPI-MakeURL2.png                               package/datalake-collector/doc/img/
	install -m 0644 doc/img/Proxy-Server-1.drawio.svg                          package/datalake-collector/doc/img/
	install -m 0644 doc/img/Proxy-Server-2.drawio.svg                          package/datalake-collector/doc/img/
	install -m 0644 doc/img/GitLab-Download.png                                package/datalake-collector/doc/img/
	install -m 0644 doc/img/T_RULE_META_0000.png                               package/datalake-collector/doc/img/
	install -m 0644 doc/img/T_BATCH_LOG_0000.png                               package/datalake-collector/doc/img/
	install -m 0644 doc/img/T_BATCH_RESULT_0000.png                            package/datalake-collector/doc/img/
	install -m 0644 lib/datalake-collector-1.0.2.jar                           package/datalake-collector/lib/
	install -m 0644 lib/hive-jdbc-2.3.7-standalone.jar                         package/datalake-collector/lib/
	install -m 0644 lib/hive-jdbc-3.1.2-standalone.jar                         package/datalake-collector/lib/
	install -m 0644 lib/Cloudera-JDBC-Driver-for-Apache-Hive-Install-Guide.pdf package/datalake-collector/lib/
	install -m 0644 lib/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf      package/datalake-collector/lib/
	install -m 0644 lib/HiveJDBC4.jar                                          package/datalake-collector/lib/
	install -m 0644 lib/HiveJDBC41.jar                                         package/datalake-collector/lib/
	install -m 0644 lib/HiveJDBC42.jar                                         package/datalake-collector/lib/
	install -m 0644 lib/ImpalaJDBC4.jar                                        package/datalake-collector/lib/
	install -m 0644 lib/ImpalaJDBC41.jar                                       package/datalake-collector/lib/
	install -m 0644 lib/ImpalaJDBC42.jar                                       package/datalake-collector/lib/
	install -m 0644 lib/ojdbc6.jar                                             package/datalake-collector/lib/
	install -m 0644 lib/tibero6-jdbc.jar                                       package/datalake-collector/lib/
	install -m 0644 lib/mssql-jdbc-8.2.2.jre8.jar                              package/datalake-collector/lib/
	install -m 0644 lib/JDBC-10.2-latest-cubrid.jar                            package/datalake-collector/lib/
	install -m 0644 lib/mysql-connector-java-5.1.49.jar                        package/datalake-collector/lib/
	install -m 0644 lib/postgresql-42.2.16.jar                                 package/datalake-collector/lib/
	install -m 0644 openapi/openapi.py                                         package/datalake-collector/openapi/
	install -m 0755 openapi/template1.py                                       package/datalake-collector/openapi/
	install -m 0755 openapi/template2.py                                       package/datalake-collector/openapi/
	install -m 0755 openapi/template3.py                                       package/datalake-collector/openapi/
	install -m 0755 python/pip-install.sh                                      package/datalake-collector/python/
	install -m 0755 python/pip-uninstall.sh                                    package/datalake-collector/python/
	install -m 0644 python/six-1.15.0-py2.py3-none-any.whl                     package/datalake-collector/python/
	install -m 0644 python/python_dateutil-2.8.1-py2.py3-none-any.whl          package/datalake-collector/python/
	install -m 0644 python/xmltodict-0.12.0-py2.py3-none-any.whl               package/datalake-collector/python/
	install -m 0644 sql/schema.sql                                             package/datalake-collector/sql/
	install -m 0755 src/buildcfg.py                                            package/datalake-collector/src/
	install -m 0755 src/createcfg.py                                           package/datalake-collector/src/
	install -m 0755 src/buildrule.py                                           package/datalake-collector/src/
	install -m 0755 src/backuprule.py                                          package/datalake-collector/src/
	install -m 0755 src/restorerule.py                                         package/datalake-collector/src/
	install -m 0755 src/updaterule.py                                          package/datalake-collector/src/
	install -m 0755 src/buildrule.py                                           package/datalake-collector/src/
	install -m 0755 src/healthcheck.py                                         package/datalake-collector/src/
	install -m 0755 src/batchcheck.py                                          package/datalake-collector/src/
	install -m 0755 src/batchrun.py                                            package/datalake-collector/src/
	install -m 0755 src/batchresult.py                                         package/datalake-collector/src/
	install -m 0644 src/load000.py                                             package/datalake-collector/src/
	install -m 0644 src/load001.py                                             package/datalake-collector/src/
	install -m 0644 src/load002.py                                             package/datalake-collector/src/
	install -m 0644 src/load003.py                                             package/datalake-collector/src/
	install -m 0755 src/load004.py                                             package/datalake-collector/src/
	install -m 0755 src/csv2jdbc.py                                            package/datalake-collector/src/
	install -m 0755 src/csv2seqfile.py                                         package/datalake-collector/src/
	install -m 0644 src/csvutil.py                                             package/datalake-collector/src/
	install -m 0755 src/hiveanalyze.py                                         package/datalake-collector/src/
	install -m 0755 src/hivecreatetable.py                                     package/datalake-collector/src/
	install -m 0755 src/hivedroptable.py                                       package/datalake-collector/src/
	install -m 0755 src/hivetruncatetable.py                                   package/datalake-collector/src/
	install -m 0755 src/hivecreatepartition.py                                 package/datalake-collector/src/
	install -m 0755 src/hiveclearpartition.py                                  package/datalake-collector/src/
	install -m 0755 src/hivedroppartition.py                                   package/datalake-collector/src/
	install -m 0755 src/hivelistpartition.py                                   package/datalake-collector/src/
	install -m 0755 src/hive2csv.py                                            package/datalake-collector/src/
	install -m 0755 src/hive2jdbc.py                                           package/datalake-collector/src/
	install -m 0755 src/hivecount.py                                           package/datalake-collector/src/
	install -m 0755 src/hivequery.py                                           package/datalake-collector/src/
	install -m 0755 src/hiveshell.py                                           package/datalake-collector/src/
	install -m 0755 src/hiveverify.py                                          package/datalake-collector/src/
	install -m 0644 src/jdbc2json.py                                           package/datalake-collector/src/
	install -m 0644 src/jdbcconf.py                                            package/datalake-collector/src/
	install -m 0644 src/jdbcutil.py                                            package/datalake-collector/src/
	install -m 0755 src/jdbc2csv.py                                            package/datalake-collector/src/
	install -m 0755 src/jdbc2jdbc.py                                           package/datalake-collector/src/
	install -m 0755 src/jdbc2seqfile.py                                        package/datalake-collector/src/
	install -m 0755 src/jdbccount.py                                           package/datalake-collector/src/
	install -m 0755 src/jdbcmeta.py                                            package/datalake-collector/src/
	install -m 0755 src/jdbcquery.py                                           package/datalake-collector/src/
	install -m 0755 src/jdbcshell.py                                           package/datalake-collector/src/
	install -m 0755 src/jdbctypes.py                                           package/datalake-collector/src/
	install -m 0755 src/jdbcversion.py                                         package/datalake-collector/src/
	install -m 0755 src/jdbctruncatetable.py                                   package/datalake-collector/src/
	install -m 0644 src/secret.py                                              package/datalake-collector/src/
	cd package; tar cvzf ../datalake-collector.tgz datalake-collector

java: all
	cd java; ./build

count:
	find openapi src java/src -name '*.java' -o -name '*.py' | xargs wc -l

clean:
	cd java; ./clean
	rm -rf *.csv *.seq .*.seq.crc
	rm -rf datalake-collector.tgz
	rm -rf cfg/IM-*
	rm -rf java/target java/.classpath java/.factorypath java/.project java/.settings
	rm -rf openapi/__pycache__ openapi/*.pyc
	rm -rf package/datalake-collector
	rm -rf src/__pycache__ src/*.pyc
	rm -rf test/data/*.db test/data/*.db-journal test/out/*
	rm -rf tmp/test.db tmp/*.csv
