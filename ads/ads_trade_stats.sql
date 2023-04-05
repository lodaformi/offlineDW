DROP TABLE IF EXISTS ads.ads_trade_stats;
CREATE EXTERNAL TABLE ads.ads_trade_stats
(
    `dt` STRING COMMENT '统计日期',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单总额,GMV',
    `sku_count` BIGINT COMMENT '下单总件数',
    `order_user_count` BIGINT COMMENT '下单人数',
    `order_count` BIGINT COMMENT '下单次数'
) COMMENT '交易统计'
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_trade_stats';

WITH orders AS (
 SELECT
    count(DISTINCT sn) order_count
    FROM
    dwd.dwd_yjx_order_zip
    WHERE dt="2021-01-01" AND rng_end_dt="9999-12-31"
)
INSERT INTO TABLE ads.ads_trade_stats
SELECT
    "2021-01-01" dt,
    sum(order_total_amount_1d) order_total_amount,
    sum(order_num_1d) sku_count,
       count(DISTINCT member_id) order_user_count,
       -- 因为上面的字段都是聚合函数，这里不能是非聚合字段，使用max函数，不影响原来的值
       max(order_count) order_count
FROM
dws.dws_trade_user_sku_order_1d
    -- 当联表最终生成的表只有一行数据时，可以不用写联表条件
    LEFT JOIN orders
WHERE dt="2021-01-01";