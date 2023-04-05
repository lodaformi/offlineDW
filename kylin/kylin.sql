create database if not exists kylin;

drop table if exists kylin.kylin_sale;

create external table kylin.kylin_sale(
    id int,
    saledate date,
    market string,
    category string,
    item string,
    number int,
    sales int
)
row format delimited fields terminated by ','
lines terminated by '\n';

load data inpath "hdfs://hdfs-yjx/root/kylin_sale1.txt" overwrite into table kylin.kylin_sale;