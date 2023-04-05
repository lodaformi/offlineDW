drop table if exists ods.ods_yjx_order_inc;
drop table ods.ods_yjx_order_inc;
create external table IF NOT EXISTS ods.ods_yjx_order_inc(
    id bigint comment 'ID',
    create_time String comment '创建时间',
    update_time String comment '修改时间',
    can_return String comment '订单是否支持原路退回',
    cancel_reason String comment '订单取消原因',
    client_type String comment '订单来源',
    complete_time String comment '完成时间',
    consignee_address_id_path String comment '地址id,分割',
    consignee_address_path String comment '地址名称,分割"',
    consignee_detail String comment '详细地址',
    consignee_mobile String comment '收件人手机',
    consignee_name String comment '收件人姓名',
    deliver_status String comment '货运状态',
    discount_price String comment '优惠金额',
    flow_price String comment '总价格',
    freight_price String comment '运费',
    goods_num String comment '商品数量',
    goods_price String comment '商品价格',
    logistics_code String comment '物流公司CODE',
    logistics_name String comment '物流公司名称',
    logistics_no String comment '发货单号',
    logistics_time String comment '送货时间',
    member_id String comment '会员ID',
    need_receipt String comment '是否需要发票',
    order_status String comment '订单状态',
    order_type String comment '订单类型',
    parent_order_sn String comment '是否为其他订单下的订单，如果是则为依赖订单的sn，否则为空',
    pay_order_no String comment '支付方式返回的交易号',
    pay_status String comment '付款状态',
    payment_method String comment '支付方式',
    payment_time String comment '支付时间',
    price_detail String comment '价格详情',
    promotion_id String comment '是否为某订单类型的订单，如果是则为订单类型的id，否则为空',
    remark String comment '买家订单备注',
    store_id String comment '店铺ID',
    sn String comment '订单编号',
    trade_sn String comment '交易编号 关联Trade',
    weight String comment '订单商品总重量',
    qr_code String comment '提货码',
    distribution_id String comment '分销员ID',
    delivery_method String comment '配送方式'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_order_inc';

msck repair table ods.ods_yjx_order_inc;