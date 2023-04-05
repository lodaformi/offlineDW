DROP TABLE IF EXISTS ads.ads_yjx_goods_category_topN_day;
create external table IF NOT EXISTS ads.ads_yjx_goods_category_topN_day(
    goods_id bigint comment '商品ID',
    category_id bigint comment '分类id',
    category_name String comment '分类名称',
    goods_name String comment '商品名',
    goods_total String comment '商品销量',
    order_num int comment '排名',
    dt String comment '日期'
)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_yjx_goods_category_topN_day';

INSERT INTO TABLE ads.ads_yjx_goods_category_topN_day
SELECT
        goods_id,
    cast(category1_id as bigint) category1_id,
       category_name,
       goods_name,
        goods_total,
       order_num,
       "2021-01-01" dt
FROM (
    SELECT
                goods_id,
               category1_id,
               category_name,
               goods_name,
                goods_total,
           row_number() over (partition by category1_id order by cast(goods_total as bigint) desc ) order_num
    FROM(
        SELECT
                goods_id,
               category1_id,
               cf.name category_name,
               goods_name,
               goods_total
        FROM
        dws.dws_yjx_goods_topic gt
        LEFT JOIN ods.ods_yjx_category_full cf
        ON gt.category1_id = cast(cf.id as string)
        WHERE gt.dt="2021-01-01" and cf.dt="2021-01-01"
    ) a
) b
where order_num <=10;