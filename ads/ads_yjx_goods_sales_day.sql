DROP TABLE IF EXISTS ads.ads_yjx_goods_sales_day;
create external table IF NOT EXISTS ads.ads_yjx_goods_sales_day(
    goods_all_total bigint comment '商品销售总量',
    goods_all_sale bigint comment '商品销售总额',
    dt String comment '日期'
)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_yjx_goods_sales_day';


INSERT into table ads.ads_yjx_goods_sales_day
SELECT
    sum(goods_total) goods_all_total,
    sum(goods_sale ) goods_all_sale,
    dt
FROM dws.dws_yjx_goods_topic
where dt ='2021-01-01'
group by dt