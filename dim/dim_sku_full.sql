-- 商品维度表
drop table dim.dim_sku_full;
create external table if not exists dim.dim_sku_full
(
    id string,
    sku_name string,
    goods_id string,
    brand_id string,
    brand_name string,
    category1_id string,
    category1_name string,
    category2_id string,
    category2_name string,
    category3_id string,
    category3_name string,
    sku_price decimal(16,2)
) partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dim/dim_sku_full';

-- noinspection SqlInsertValues

with brand as (
    select
            id,
           name
    from
    ods.ods_yjx_brand_full
    where dt = '2021-01-01'
),
     category as (
         select
                -- 数据类型转换为string，与sku.category_id才能匹配正确
                cast(id as string) id,
                name,
                level
         from
         ods.ods_yjx_category_full
         where dt = '2021-01-01'
     ),
     sku as (
         select
             id,
            goods_name sku_name,
            goods_id,
            category_id,
            brand_id,
            price
         from
         ods.ods_yjx_goods_sku_full
             -- 列转行
         lateral view explode(split(category_path, ",")) t as category_id
         where id is not null
         and dt = "2021-01-01"
     )
insert overwrite table dim.dim_sku_full partition (dt="2021-01-01")
select
        sku.id,
        sku_name,
       goods_id,
       brand_id,
       brand.name  brand_name,
       -- 分组聚合实现行转列！！！！
        max(case when category.level = 0 then category_id end) category1_id,
        max(case when category.level = 0 then category.name end) category1_name,
        max(case when category.level = 1 then category_id end) category2_id,
        max(case when category.level = 1 then category.name end) category2_name,
        max(case when category.level = 2 then category_id end) category3_id,
        max(case when category.level = 2 then category.name end) category3_name,
       price sku_price
from
sku
left join brand
    on sku.brand_id = brand.id
left join category
    on sku.category_id = category.id
where sku_name is not null
-- 分组
group by sku.id,sku_name,goods_id, brand_id, brand.name, price;