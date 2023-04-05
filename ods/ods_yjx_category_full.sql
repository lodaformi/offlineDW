drop table ods.ods_yjx_category_full;
create external table IF NOT EXISTS ods.ods_yjx_category_full(
    id bigint comment '分类ID',
    commission_rate String comment '佣金比例',
    image String comment '分类图标',
    level String comment '层级',
    name String comment '分类名称',
    parent_id String comment '父ID',
    sort_order String comment '排序值'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_category_full';

msck repair table ods.ods_yjx_category_full;