create database dq;
create table dq.dq_dwd_app_event_detail_area(
    dt string,
    province_null bigint,
    province_notnull bigint,
    city_null bigint,
    city_notnull bigint,
    region_null bigint,
    region_notnull bigint,
    table_record_cnt bigint
)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dq/dq_dwd_app_event_detail_area';

INSERT INTO TABLE  dq.dq_dwd_app_event_detail_area
SELECT
    "2021-02-01" dt,
    count(`if`(province is null, 1, null)) province_null,
    count(`if`(province is not null, 1, null)) province_null,
    count(`if`(city is null, 1, null)) province_null,
    count(`if`(city is not null, 1, null)) province_null,
    count(`if`(region is null, 1, null)) province_null,
    count(`if`(region is not null, 1, null)) province_null,
    count(1) table_record_cnt
FROM
    dwd.dwd_app_event_detail
WHERE dt="2021-02-01";
