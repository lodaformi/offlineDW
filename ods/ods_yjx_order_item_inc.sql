drop table ods.ods_yjx_order_item_inc;
create external table IF NOT EXISTS ods.ods_yjx_order_item_inc(
    id String comment 'ID',
    create_time String comment '创建时间',
    after_sale_status String comment '售后状态',
    category_id String comment '分类ID',
    comment_status String comment '评论状态',
    complain_id String comment '交易投诉ID',
    complain_status String comment '投诉状态',
    flow_price String comment '实际金额',
    goods_price String comment '销售金额',
    goods_id String comment '商品ID',
    image String comment '图片',
    goods_name String comment '商品名称',
    num String comment '销售量',
    order_sn String comment '订单编号',
    price_detail String comment '价格详情',
    sku_id String comment '货品ID',
    sn String comment '子订单编号',
    specs String comment '规格json',
    trade_sn String comment '交易编号',
    unit_price String comment '单价',
    sub_total String comment '小记'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_order_item_inc';

msck repair table ods.ods_yjx_order_item_inc;

