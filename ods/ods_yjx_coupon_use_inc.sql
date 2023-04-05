DROP TABLE IF EXISTS ods.ods_yjx_coupon_use_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_yjx_coupon_use_inc
(
    id STRING COMMENT '购物券编号',
    coupon_id STRING COMMENT '购物券编号',
    member_id STRING COMMENT '用户编号',
    order_sn STRING,
    coupon_status STRING,
    get_time STRING,
    using_time STRING,
    used_time STRING,
    expire_time STRING
)COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
LOCATION 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_coupon_use_inc';

msck repair table ods.ods_yjx_coupon_use_inc;