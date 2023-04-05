drop table if exists ads.ads_userprofile_consume_tag;
create table ads.ads_userprofile_consume_tag(
    member_id                       bigint ,--用户
    first_order_time                string ,--首单日期
    last_order_time                 string ,--末单日期
    first_order_ago                 bigint ,--首单距今时间
    last_order_ago                   bigint ,--末单距今时间
    month1_order_cnt                bigint ,--近30天下单次数
    month1_order_amt                double ,--近30天购买金额（总金额）
    month2_order_cnt                bigint ,--近60天购买次数
    month2_order_amt                double ,--近60天购买金额
    month3_order_cnt                bigint ,--近90天购买次数
    month3_order_amt                double ,--近90天购买金额
    max_order_amt                    double ,--最大订单金额
    min_order_amt                   double ,--最小订单金额
    total_order_cnt                 bigint ,--累计消费次数（不含退拒）
    total_order_amt                 double ,--累计消费金额（不含退拒）
    total_coupon_amt                double ,--累计使用代金券金额
    common_address                  string ,--常用收货地址
    common_paytype                  string ,--常用支付方式
    month1_cart_cnt_30              bigint ,--最近30天加购次数
    month1_cart_goods_cnt_30         bigint --最近30天加购商品件数
) partitioned by (dt string)
stored as parquet
LOCATION 'hdfs://hdfs-yjx/yjx/app/ads/ads_userprofile_consume_tag';

with tmp1 as (
SELECT
       member_id,
       min(create_time) first_order_time,
       max(create_time) last_order_time,
       count(if(datediff('2021-01-01',create_time)<=30,1,null)) month1_order_cnt ,
       sum(if(datediff('2021-01-01',create_time)<=30,flow_price-nvl(discount_price,0),null)) month1_order_amt,
       count(if(datediff('2021-01-01',create_time)<=60,1,null)) month2_order_cnt ,
       sum(if(datediff('2021-01-01',create_time)<=60,flow_price-nvl(discount_price,0),null)) month2_order_amt,
       count(`if`(datediff("2021-01-1",create_time)<=90,1,null)) month3_order_cnt,
       sum(`if`(datediff("2021-01-1",create_time)<=90,flow_price-nvl(discount_price,0),null)) month3_order_amt,
       max(flow_price-nvl(discount_price,0)) max_order_amt,
       min(flow_price-nvl(discount_price,0)) min_order_amt,
       count(1) total_order_cnt,
       sum(flow_price-nvl(discount_price,0)) total_order_amt,
       sum(nvl(discount_price,0)) total_coupon_amt
FROM
dwd.dwd_yjx_order_zip
-- where能过滤出dt时间当天，所有没有关闭的订单(状态不是完成)和已经结束(rng_end_dt="9999-12-31")的订单
-- 只要根据rng_end_dt="9999-12-31"就能取出一个用户一条订单中最新的一个状态，一个订单不可能存在两个状态是9999的数据
WHERE dt="2021-01-01" AND rng_end_dt="9999-12-31"
GROUP BY member_id ),

tmp2 as (
SELECT
   member_id,
   add as common_address
FROM (
     SELECT member_id,
            add,
            row_number() over (partition by add order by cnt desc) r
     FROM (
              SELECT member_id,
                     consignee_address_path add,
                     count(1)               cnt
              FROM dwd.dwd_yjx_order_zip
              WHERE dt = "2021-01-01"
                AND rng_end_dt = "9999-12-31"
              GROUP BY member_id, consignee_address_path
         ) a
     )b
WHERE r=1 ),

tmp3 as (
SELECT
   member_id,
   payment_method as common_paytype
FROM (
     SELECT member_id,
            payment_method,
            row_number() over (partition by payment_method order by cnt desc) r
     FROM (
              SELECT member_id,
                     payment_method ,
                     count(1)  cnt
              FROM dwd.dwd_yjx_order_zip
              WHERE dt = "2021-01-01"
                AND rng_end_dt = "9999-12-31"
              GROUP BY member_id, payment_method
         ) a
     )b
WHERE r=1),

tmp4 as (
SELECT
        member_id,
       count(1) month1_cart_cnt_30,
       sum(sku_num) month1_cart_goods_cnt_30
FROM
    dwd.dwd_trade_cart_inc
WHERE datediff("2021-01-01", dt)<=30
GROUP BY member_id),

-- 将商品加入购物车的人不一定会下单，
-- 下单的人不一定会加入购物车，要将这两种人都选出来
tmp5 as (
SELECT member_id
FROM
dwd.dwd_yjx_order_zip where dt="2021-01-01" and rng_end_dt="9999-12-31"
union -- 对member_id去重
SELECT member_id
FROM
dwd.dwd_trade_cart_inc WHERE datediff("2021-01-01", dt)<=30 )

INSERT OVERWRITE TABLE ads.ads_userprofile_consume_tag PARTITION (dt="2021-01-01")
SELECT
    TMP5.member_id,
     first_order_time
    ,last_order_time
    ,datediff("2021-01-01", first_order_time) first_order_ago
    ,datediff("2021-01-01", last_order_time) last_order_ago
    ,month1_order_cnt
    ,month1_order_amt
    ,month2_order_cnt
    ,month2_order_amt
    ,month3_order_cnt
    ,month3_order_amt
    ,max_order_amt
    ,min_order_amt
    ,total_order_cnt
    ,total_order_amt
    ,total_coupon_amt
    ,common_address
    ,common_paytype
    ,month1_cart_cnt_30
    ,month1_cart_goods_cnt_30
FROM
tmp5 LEFT JOIN tmp1 ON tmp5.member_id = tmp1.member_id
LEFT JOIN tmp2 ON tmp5.member_id = tmp2.member_id
LEFT JOIN tmp3 ON tmp5.member_id = tmp3.member_id
LEFT JOIN tmp4 ON tmp5.member_id = TMP4.member_id