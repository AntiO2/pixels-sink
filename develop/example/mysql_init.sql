-- init.sql

GRANT ALL PRIVILEGES ON pixels_realtime_crud.* TO 'pixels'@'%';
GRANT FILE on *.* to pixels@'%';

FLUSH PRIVILEGES;



-- create & load tpch tables
USE pixels_realtime_crud;


SOURCE /var/lib/mysql-files/sql/dss.ddl;

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/customer.tbl'
INTO TABLE CUSTOMER
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/lineitem.tbl'
INTO TABLE LINEITEM
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/nation.tbl'
INTO TABLE NATION
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/orders.tbl'
INTO TABLE ORDERS
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/part.tbl'
INTO TABLE PART
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/partsupp.tbl'
INTO TABLE PARTSUPP
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/region.tbl'
INTO TABLE REGION
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/supplier.tbl'
INTO TABLE SUPPLIER
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

