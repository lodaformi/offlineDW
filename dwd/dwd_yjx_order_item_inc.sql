DROP TABLE IF EXISTS dwd.dwd_yjx_order_item_inc;
CREATE EXTERNAL TABLE dwd.dwd_yjx_order_item_inc
(
    id          string comment '编号',
    order_sn            string comment '订单编号',
    member_id           string comment '用户编号',
    sku_id          string comment '商品id',
    create_time             string comment '创建时间',
    benefit_price           string comment '优惠价格',
    goods_price             string comment '商品价格',
    sku_num             string comment '商品数量',
    coupon_amount            DECIMAL(16, 2) comment '优惠总金额',
    original_amount             DECIMAL(16, 2) comment '原价总金额',
    total_price            DECIMAL(16, 2) comment '总金额'
) COMMENT '订单明细表'
PARTITIONED BY (`dt` STRING)
STORED AS parquet
LOCATION 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_yjx_order_item_inc';


with order_info  as (
    select
        sn,
       member_id
    from
        ods.ods_yjx_order_inc
    where dt="2021-01-01"
),
 order_item as (
     select
         id,
        order_sn,
        create_time,
        flow_price,
        goods_price,
        sku_id,
        num
     from
          ods.ods_yjx_order_item_inc
     where dt="2021-01-01"
 ),
 coupon as (
     select
     id,
     benefit_amount
     from ods.ods_yjx_coupon_info_full
     where dt="2021-01-01"
 ),
 coupon_use as (
     select
     coupon_id,
            order_sn
     from
     ods.ods_yjx_coupon_use_inc
     where dt="2021-01-01"
 )
insert overwrite table dwd.dwd_yjx_order_item_inc partition (dt="2021-01-01")
select
        order_item.id,
       order_item.order_sn,
       member_id,
       sku_id,
       create_time,
       -- 单个商品的优惠价格
       -- 并不是所有的商品都会用到优惠卷
       flow_price - nvl(benefit_amount, 0)/num benefit_price,
       goods_price,
       num sku_num,
       nvl(benefit_amount, 0) coupon_amount,
       flow_price*num original_amount,
       flow_price*num - nvl(benefit_amount, 0) total_price
from
    order_item
    left join order_info
    on order_info.sn = order_item.order_sn
    left join coupon_use
    on order_item.order_sn = coupon_use.order_sn
    left join coupon
    on coupon_use.coupon_id = coupon.id;