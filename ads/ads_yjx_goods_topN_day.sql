DROP TABLE IF EXISTS ads.ads_yjx_goods_topN_day;
create external table IF NOT EXISTS ads.ads_yjx_goods_topN_day(
    goods_id bigint comment '商品ID',
    goods_name String comment '商品名',
    goods_total String comment '商品销量',
    -- 与dws_yjx_goods_topic比较，增加的字段
    order_num int comment '排名',
    dt String comment '日期'
)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_yjx_goods_topN_day';



insert into table ads.ads_yjx_goods_topN_day
select
    goods_id,
    goods_name,
    goods_total,
    row_number() over (order by cast(goods_total as int) desc) order_num,
    dt
from
    dws.dws_yjx_goods_topic
where dt='2021-01-01' and goods_total is not null
limit 10;