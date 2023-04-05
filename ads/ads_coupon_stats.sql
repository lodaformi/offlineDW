DROP TABLE IF EXISTS ads.ads_coupon_stats;
CREATE EXTERNAL TABLE ads.ads_coupon_stats
(
    `dt`          STRING COMMENT '统计日期',
    `coupon_id`   STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    --
    `reduce_rate` DECIMAL(16, 4) COMMENT '补贴率'
) COMMENT '优惠券统计'
    STORED AS parquet
    LOCATION 'hdfs://hdfs-yjx/yjx/app/ads/ads_coupon_stats';

insert overwrite table ads.ads_coupon_stats
select
    '2021-01-01' dt,
    coupon_id,
    dtco30d.coupon_name,
    dcf.create_time start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,4))
from dws.dws_trade_coupon_order_30d dtco30d
left join dim.dim_coupon_full dcf
on dtco30d.coupon_id = dcf.id
where dtco30d.dt='2021-01-01';