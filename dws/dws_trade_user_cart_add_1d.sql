DROP TABLE IF EXISTS dws.dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws.dws_trade_user_cart_add_1d
(
    `member_id`         STRING COMMENT '用户id',
    `sku_id`            STRING COMMENT '加购的sku_id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数',
    `province`          STRING COMMENT '省份',
    `city`              STRING COMMENT '城市',
    `region`             STRING COMMENT '地区',
    `osname`            STRING COMMENT '系统名称'
) COMMENT '交易域用户粒度加购最近1日汇总事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION 'hdfs://hdfs-yjx/yjx/app/dws/dws_trade_user_cart_add_1d'
TBLPROPERTIES ('orc.compress' = 'snappy');

WITH tmp1 as (
    SELECT
            member_id,
           sku_id,
           count(`if`(datediff("2021-01-1", dt)<=1, 1, null)) cart_add_count_1d,
           sum(`if`(datediff("2021-01-1", dt)<=1,sku_num, null)) cart_add_num_1d
    FROM
        dwd.dwd_trade_cart_inc
    WHERE dt="2021-01-01"
    group by member_id, sku_id
    ),
tmp2 as (
    SELECT
            account,
           province,
           city,
           region,
           osname
    FROM
    dwd.dwd_app_event_detail
    WHERE dt="2021-02-01" AND eventid="addCart"
)
SELECT
        tmp1.member_id,
       sku_id,
       tmp1.cart_add_count_1d,
       cart_add_num_1d,
       province,
       city,
       region,
       osname
FROM
tmp1 LEFT JOIN tmp2
ON tmp1.member_id = tmp2.account;

-- 方法二？
select
    member_id,
    sku_id,
    count(1) cart_add_count_1d,
    sum(sku_num) cart_add_num_1d,
    province,
    city,
    region,
    osname
from
    dwd.dwd_trade_cart_inc cart
left join
    dwd.dwd_app_event_detail event
where cart.dt='2021-01-01'
and event.dt='2021-02-01'
and cart.member_id = event.account
and event.eventid = 'addCart'
and event.properties['skuId'] = cart.sku_id
group by member_id, sku_id,province,
    city,
    region,
    osname;

SELECT * FROM
dwd.dwd_app_event_detail
WHERE dt="2021-02-01" AND eventid = 'addCart';