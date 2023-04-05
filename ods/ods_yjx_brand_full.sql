drop table ods.ods_yjx_brand_full;
create external table IF NOT EXISTS ods.ods_yjx_brand_full(
    id String comment '品牌ID',
    logo String comment '品牌图标',
    name String comment '品牌名'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_brand_full';

msck repair table ods.ods_yjx_brand_full;