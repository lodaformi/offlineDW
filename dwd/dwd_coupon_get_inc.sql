DROP TABLE IF EXISTS dwd.dwd_coupon_get_inc;
CREATE EXTERNAL TABLE dwd.dwd_coupon_get_inc
(
    `id`        STRING COMMENT '编号',
    `coupon_id` STRING COMMENT '优惠券ID',
    `member_id` STRING COMMENT 'userid',
    `get_time`  STRING COMMENT '领取时间',
    `order_sn`  STRING COMMENT '订单编码',
    `used_time` STRING COMMENT '使用时间'
) COMMENT '优惠券领取事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS parquet
    LOCATION 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_coupon_get_inc';


insert overwrite table dwd.dwd_coupon_get_inc partition (dt='2021-01-01')
select
    id,
    coupon_id,
    member_id,
    get_time,
    order_sn,
    used_time
from ods.ods_yjx_coupon_use_inc
where dt='2021-01-01';


select date_sub('2021-01-01',1)