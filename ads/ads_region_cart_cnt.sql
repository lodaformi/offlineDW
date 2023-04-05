drop table if exists ads.ads_region_cart_cnt;
create external table ads.ads_region_cart_cnt(
    `province` string COMMENT '省份',
    `city` string COMMENT '城市',
    `region` string COMMENT '地区',
    `cart_num` bigint COMMENT '加入购物车数量',
    `dt` STRING COMMENT '计算日期'
) COMMENT '商品加购数量地区分布'
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_region_cart_cnt';


insert into table ads.ads_region_cart_cnt(
select
    province,
    city,
    region,
    sum(cart_add_num_1d) cart_num,
    '2021-01-01' as dt
from
    dws.dws_trade_user_cart_add_1d
where dt='2021-01-01'
group by province, city, region;