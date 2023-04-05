drop table if exists ads.ads_product_cart_topN;
create external table ads.ads_product_cart_topN(
    `sku_id` string COMMENT '商品ID',
    `cart_num` bigint COMMENT '加入购物车数量',
    `dt` STRING COMMENT '计算日期'
) COMMENT '商品加入购物车TopN'
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_product_cart_topN';

insert into table ads.ads_product_cart_topN
select
    sku_id,
    cart_num,
    dt
from
    (
        select
            sku_id,
            sum(cart_add_num_1d) cart_num,
            '2021-01-01'  dt
        from
            dws.dws_trade_user_cart_add_1d
        where dt='2021-01-01'
        group by sku_id
    )a
order by cart_num desc
limit 10;
