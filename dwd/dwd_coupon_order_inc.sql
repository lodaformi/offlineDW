DROP TABLE IF EXISTS dwd.dwd_coupon_order_inc;
CREATE EXTERNAL TABLE dwd.dwd_coupon_order_inc
(
    `id`         STRING COMMENT '编号',
    `coupon_id`  STRING COMMENT '优惠券ID',
    `member_id`    STRING COMMENT 'member_id',
    `order_sn`   STRING COMMENT 'order_sn',
    `used_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS parquet
    LOCATION 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_coupon_order_inc';

insert overwrite table dwd.dwd_coupon_order_inc partition(dt='2021-01-01')
select
    id,
    coupon_id,
    member_id,
    order_sn,
    used_time
from ods.ods_yjx_coupon_use_inc
where dt='2021-01-01'
and used_time>='2021-01-01' and used_time<'2021-01-02';