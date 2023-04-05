-- 用户购买商品明细汇总表
DROP TABLE IF EXISTS dws.dws_trade_user_sku_order_1d;
CREATE EXTERNAL TABLE dws.dws_trade_user_sku_order_1d
(
    `member_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'sku_id',
    `goods_id` STRING COMMENT 'goods_id',
    `sku_name` STRING COMMENT 'sku名称',
    `category1_id` STRING COMMENT '一级分类id',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id` STRING COMMENT '二级分类id',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id` STRING COMMENT '三级分类id',
    `category3_name` STRING COMMENT '三级分类名称',
    `brand_id` STRING COMMENT '品牌id',
    `brand_name` STRING COMMENT '品牌名称',
    `order_count_1d` BIGINT COMMENT '最近1日下单次数',
    `order_num_1d` BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT  '最近1日优惠券优惠金额',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
PARTITIONED BY (`dt` STRING)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_trade_user_sku_order_1d';


WITH sku AS (
    SELECT
            ID,
           SKU_NAME,
           goods_id,
           BRAND_ID,
           BRAND_NAME,
           CATEGORY1_ID,
           CATEGORY1_NAME,
           CATEGORY2_ID,
           CATEGORY2_NAME,
           CATEGORY3_ID,
           CATEGORY3_NAME
    FROM dim.dim_sku_full
    WHERE dt="2021-01-01"
) ,
ord AS (
    SELECT
            member_id,
           sku_id,
           count(DISTINCT order_sn)  order_count_1d,
           sum(sku_num) order_num_1d,
           sum(original_amount) order_original_amount_1d,
           sum(nvl(coupon_amount,0)) coupon_reduce_amount_1d,
           sum(total_price) order_total_amount_1d
    FROM
    dwd.dwd_yjx_order_item_inc
    -- 注意日期
    WHERE dt="2021-01-01"
    GROUP BY member_id, sku_id
)
INSERT OVERWRITE TABLE dws.dws_trade_user_sku_order_1d PARTITION (dt="2021-01-01")
SELECT
        member_id,
       sku_id,
       goods_id,
       sku_name,category1_id,category1_name,category2_id,category2_name,
        category3_id,category3_name, brand_id, brand_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       coupon_reduce_amount_1d,
        order_total_amount_1d
FROM
    ord LEFT JOIN sku ON ord.sku_id = sku.id;