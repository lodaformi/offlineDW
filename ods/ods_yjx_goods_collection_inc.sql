drop table  ods.ods_yjx_goods_collection_inc;
create external table IF NOT EXISTS ods.ods_yjx_goods_collection_inc(
    id bigint comment 'id',
    create_time string comment '创建时间',
    member_id String comment '会员ID',
    sku_id String comment '商品ID'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_goods_collection_inc';


show partitions ods.ods_yjx_goods_collection_inc;

select * from ods.ods_yjx_goods_collection_inc;

alter table ods.ods_yjx_goods_collection_inc add if not exists partition (dt='2021-01-01');