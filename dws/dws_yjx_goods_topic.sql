-- 商品主题表
-- 从spu商品的角度，查看商品的基本信息，商品销售总额，商品销售总量，商品收藏数量
-- 所以要从sku转换到spu
DROP TABLE IF EXISTS dws.dws_yjx_goods_topic;
create external table IF NOT EXISTS dws.dws_yjx_goods_topic(
    goods_id bigint comment '商品ID',
    create_time String comment '创建时间',
    delete_flag String comment '删除标志 true/false 删除/未删除',
    auth_message String comment '审核信息',
    brand_id String comment '品牌ID',
    category1_id String comment '一级分类',
    category2_id String comment '二级分类',
    category3_id String comment '三级分类',
    cost String comment '售价',
    goods_name String comment '商品名称',
    goods_unit String comment '计量单位',
    goods_video String comment '商品视频',
    intro String comment '商品详情',
    is_auth String comment '审核状态',
    market_enable String comment '上架状态',
    mobile_intro String comment '商品移动端详情',
    original String comment '原图路径',
    price String comment '成本价格',
    sales_model String comment '销售模式',
    self_operated String comment '是否自营',
    store_id String comment '店铺ID',
    selling_point String comment '卖点',
    shop_category_path String comment '店铺分类',
    sn String comment '商品编号',
    under_message String comment '下架原因',
    weight String comment '重量',
    store_category_path String comment '店铺分类路径',
    params String comment '参数',
    goods_type String comment '商品类别',
    -- 添加的字段
    goods_sale String comment '商品销售总额',
    goods_total String comment '商品销售总量',
    -- 从ods_yjx_goods_collection_inc表中获取
    goods_collection_num String comment '商品收藏数量'
)partitioned by(dt String)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_yjx_goods_topic';


with goods as (
    select
        id goods_id
        ,create_time
        ,delete_flag
        ,auth_message
        ,brand_id
        ,category1_id
        ,category2_id
        ,category3_id
        ,cost
        ,goods_name
        ,goods_unit
        ,goods_video
        ,intro
        ,is_auth
        ,market_enable
        ,mobile_intro
        ,original
        ,price
        ,sales_model
        ,self_operated
        ,store_id
        ,selling_point
        ,shop_category_path
        ,sn
        ,under_message
        ,weight
        ,store_category_path
        ,params
        ,goods_type
    from
        dwd.dwd_yjx_goods
    where dt='2021-01-01'
),
order_goods as (
    select
        goods_id,
           -- 算出商品销售总额和销售总量
        sum(goods_sale) as goods_sale,
        sum(goods_total) as goods_total
    from
    (select
        t2.goods_id,
        sum(order_total_amount_1d) goods_sale,
        sum(order_num_1d) goods_total
    from
        dws.dws_trade_user_sku_order_1d t1
    left join dim.dim_sku_full t2
    where t1.dt='2021-02-01'
    and t1.sku_id = t2.id
    -- 以sku_id为线索，找到对应的goods_id，并求和
    group by sku_id,t2.goods_id)a
    -- 以goods_id分组，求和
    group by goods_id
),
collection as(
    select
        goods_id,
           -- 算出添加购物车的数量
        sum(goods_collection_num) as goods_collection_num
    from
    (
            select
            t2.goods_id,
            count(1) goods_collection_num
        from
            ods.ods_yjx_goods_collection_inc t1
        left join dim.dim_sku_full t2
        where t1.dt='2021-01-01'
        and t1.sku_id = t2.id
            -- 以sku_id为线索，找到对应的goods_id，并求和
        group by sku_id,t2.goods_id
    )a
    -- 以goods_id分组，求和
    group by goods_id
)

insert overwrite table dws.dws_yjx_goods_topic partition (dt='2021-01-01')
select
        goods.goods_id
        ,create_time
        ,delete_flag
        ,auth_message
        ,brand_id
        ,category1_id
        ,category2_id
        ,category3_id
        ,cost
        ,goods_name
        ,goods_unit
        ,goods_video
        ,intro
        ,is_auth
        ,market_enable
        ,mobile_intro
        ,original
        ,price
        ,sales_model
        ,self_operated
        ,store_id
        ,selling_point
        ,shop_category_path
        ,sn
        ,under_message
        ,weight
        ,store_category_path
        ,params
        ,goods_type
        ,goods_sale
        ,goods_total
        ,goods_collection_num
from
    goods
left join
    order_goods
on goods.goods_id = order_goods.goods_id
left join
    collection
on goods.goods_id = collection.goods_id;