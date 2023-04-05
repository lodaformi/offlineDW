drop table if exists ads.ads_category_cart_cnt;
create external table ads.ads_category_cart_cnt(
    `category1_name` string COMMENT '一级分类',
    `category2_name` string COMMENT '二级分类',
    `category3_name` string COMMENT '三级分类',
    `1_10_num` bigint COMMENT '1~10元加入购物车数量',
    `10_100_num` bigint COMMENT '10~100元加入购物车数量',
    `100_500_num` bigint COMMENT '100~500元加入购物车数量',
    `500_1000_num` bigint COMMENT '500~1000元加入购物车数量',
    `1000+_num` bigint COMMENT '1000+元加入购物车数量',
    `dt` STRING COMMENT '计算日期'
) COMMENT '商品加购数量地区分布'
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_category_cart_cnt';

insert into table ads.ads_category_cart_cnt
select
    category1_name,
    category2_name,
    category3_name,
    sum(if( sku_price >=1 and sku_price < 10 ,cart_add_num_1d,0)) 1_10_num,
    sum(if( sku_price >=10 and sku_price < 100 ,cart_add_num_1d,0)) 10_100_num,
    sum(if( sku_price >=100 and sku_price < 500 ,cart_add_num_1d,0)) 100_500_num,
    sum(if( sku_price >=500 and sku_price < 1000 ,cart_add_num_1d,0)) 500_1000_num,
    sum(if( sku_price >=1000  ,cart_add_num_1d,0)) `1000+_num`,
    sku.dt
from
    dws.dws_trade_user_cart_add_1d cart
left join
    dim.dim_sku_full sku
where cart.dt='2021-01-01'
and sku.dt='2021-01-01'
and cart.sku_id = sku.id
group by category1_name, category2_name, category3_name, sku.dt