DROP TABLE IF EXISTS dws.dws_trade_coupon_order_30d;
CREATE EXTERNAL TABLE dws.dws_trade_coupon_order_30d
(
    `coupon_id`                STRING COMMENT '优惠券id',
    `coupon_name`              STRING COMMENT '优惠券名称',
    `coupon_type_code`         STRING COMMENT '优惠券类型id',
    `coupon_type_name`         STRING COMMENT '优惠券类型名称',
    `coupon_rule`              STRING COMMENT '优惠券规则',
    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额'
) COMMENT '交易域优惠券粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS parquet
    LOCATION 'hdfs://hdfs-yjx/yjx/app/dws/dws_trade_coupon_order_30d';

insert into table dws.dws_trade_coupon_order_30d partition(dt='2021-01-01')
select
    id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    coupon_rule,
    sum(flow_price) original_amount_30d,
    sum(discount_price) coupon_reduce_amount_30d
from
    (
        select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule coupon_rule
    from
        dim.dim_coupon_full
    where dt='2021-01-01'
        )a
left join
    (
        select
        order_sn,
        coupon_id
    from
        dwd.dwd_coupon_order_inc
    where dt>=date_sub('2021-01-01',29)
    and dt<='2021-01-01'
        )b
on a.id = b.coupon_id
left join
    (
        select
        discount_price,
        flow_price,
        sn
    from
        dwd.dwd_yjx_order_zip
    where dt='2021-01-01'
    and rng_end_dt='9999-12-31'
    and rng_start_dt>=date_sub('2021-01-01',29)
    and rng_start_dt<='2021-01-01'
    and discount_price > 0
        )c
on b.order_sn = c.sn
group by id, coupon_name,
    coupon_type_code,
    coupon_type_name,
    coupon_rule;