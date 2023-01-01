# Bigdata
sqoop commands
Sqoop Import
--------------
Read RDBMS(MYSQL) data

bin/sqoop-eval --connect jdbc:mysql://localhost/test -username root -password root --query "SELECT * FROM demo"

MYSQL to HDFS

bin/sqoop import --connect jdbc:mysql://localhost/test -username root -password root --table test2 -m1 hadoop fs -ls

--target-dir

--sqoop import-all-tables

MYSQL to Hive

bin/sqoop-import --connect jdbc:mysql://localhost/test -username root -password root --table test --hive-table mysqltohive2 --create-hive-table --hive-import -m1

MYSQL to HBase

bin/sqoop import --connect jdbc:mysql://localhost/test --username root --password root --table demo --hbase-table testhbase --column-family cf --hbase-row-key pid --hbase-create-table -m1

Sqoop Export
------------
HDFS to MYSQL

bin/sqoop export --connect jdbc:mysql://localhost/test --username root -password root --table name_3 --export-dir /user/hive/warehouse/test.db/name.txt -m1

Hive to MYSQL

bin/sqoop export --connect jdbc:mysql://localhost/test --table test_hive_1 --export-dir /user/hive/warehouse/test.db/demo --username root -password root -m1
