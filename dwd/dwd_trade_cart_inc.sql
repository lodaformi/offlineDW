DROP TABLE IF EXISTS dwd.dwd_trade_cart_inc;
CREATE EXTERNAL TABLE dwd.dwd_trade_cart_inc
(
    `id` STRING COMMENT '编号',
    `member_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num` BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
PARTITIONED BY (`dt` STRING)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_trade_cart_inc';


insert overwrite table dwd.dwd_trade_cart_inc partition(dt='2021-01-03')
select
    id,
    member_id,
    sku_id,
    sku_name,
    sku_num
from ods.ods_yjx_cart_info_inc
where dt='2021-01-03'
and is_ordered='0';