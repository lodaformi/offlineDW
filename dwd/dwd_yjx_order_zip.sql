create external table IF NOT EXISTS dwd.dwd_yjx_order_zip(
    id                                      bigint comment 'ID',
    create_time                           String comment '创建时间',
    update_time                           String comment '更新时间',
    can_return                              String comment '订单是否支持原路退回',
    cancel_reason                         String comment '订单取消原因',
    client_type                           String comment '订单来源',
    complete_time                         String comment '完成时间',
    consignee_address_id_path             String comment '地址id ,分割',
    consignee_address_path                 String comment '地址名称 ,分割"',
    consignee_detail                          String comment '详细地址',
    consignee_mobile                          String comment '收件人手机',
    consignee_name                        String comment '收件人姓名',
    deliver_status                        String comment '货运状态',
    discount_price                        String comment '优惠金额',
    flow_price                          String comment '总价格',
    freight_price                         String comment '运费',
    goods_num                           String comment '商品数量',
    goods_price                           String comment '商品价格',
    logistics_code                        String comment '物流公司CODE',
    logistics_name                        String comment '物流公司名称',
    logistics_no                          String comment '发货单号',
    logistics_time                        String comment '送货时间',
    member_id                           String comment '会员ID',
    need_receipt                          String comment '是否需要发票',
    order_status                          String comment '订单状态',
    order_type                          String comment '订单类型',
    parent_order_sn                        String comment '是否为其他订单下的订单，如果是则为依赖订单的sn，否则为空',
    pay_order_no                          String comment '支付方式返回的交易号',
    pay_status                          String comment '付款状态',
    payment_method                        String comment '支付方式',
    payment_time                          String comment '支付时间',
    price_detail                          String comment '价格详情',
    promotion_id                          String comment '是否为某订单类型的订单，如果是则为订单类型的id，否则为空',
    remark                              String comment '买家订单备注',
    store_id                           String comment '店铺ID',
    sn                                  String comment '订单编号',
    trade_sn                          String comment '交易编号 关联Trade',
    weight                           String comment '订单商品总重量',
    qr_code                           String comment '提货码',
    distribution_id                           String comment '分销员ID',
    delivery_method                        String comment '配送方式',
    rng_start_dt                          String comment '拉链区间开始日期',
    rng_end_dt                        String comment '拉链区间结束日期'
)partitioned by(dt String)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_yjx_order_zip';

INSERT INTO TABLE dwd.dwd_yjx_order_zip PARTITION (dt="2021-01-02")
SELECT
    zip.id
    ,zip.create_time
    ,zip.update_time
    ,zip.can_return
    ,zip.cancel_reason
    ,zip.client_type
    ,zip.complete_time
    ,zip.consignee_address_id_path
    ,zip.consignee_address_path
    ,zip.consignee_detail
    ,zip.consignee_mobile
    ,zip.consignee_name
    ,zip.deliver_status
    ,zip.discount_price
    ,zip.flow_price
    ,zip.freight_price
    ,zip.goods_num
    ,zip.goods_price
    ,zip.logistics_code
    ,zip.logistics_name
    ,zip.logistics_no
    ,zip.logistics_time
    ,zip.member_id
    ,zip.need_receipt
    ,zip.order_status
    ,zip.order_type
    ,zip.parent_order_sn
    ,zip.pay_order_no
    ,zip.pay_status
    ,zip.payment_method
    ,zip.payment_time
    ,zip.price_detail
    ,zip.promotion_id
    ,zip.remark
    ,zip.store_id
    ,zip.sn
    ,zip.trade_sn
    ,zip.weight
    ,zip.qr_code
    ,zip.distribution_id
    ,zip.delivery_method
    ,zip.rng_start_dt
    ,`if`(zip.rng_end_dt = "9999-12-31" and inc.sn is not null,
    date_sub("2021-01-02",1), zip.rng_end_dt) rng_end_dt
FROM
    (SELECT
    id
    ,create_time
    ,update_time
    ,can_return
    ,cancel_reason
    ,client_type
    ,complete_time
    ,consignee_address_id_path
    ,consignee_address_path
    ,consignee_detail
    ,consignee_mobile
    ,consignee_name
    ,deliver_status
    ,discount_price
    ,flow_price
    ,freight_price
    ,goods_num
    ,goods_price
    ,logistics_code
    ,logistics_name
    ,logistics_no
    ,logistics_time
    ,member_id
    ,need_receipt
    ,order_status
    ,order_type
    ,parent_order_sn
    ,pay_order_no
    ,pay_status
    ,payment_method
    ,payment_time
    ,price_detail
    ,promotion_id
    ,remark
    ,store_id
    ,sn
    ,trade_sn
    ,weight
    ,qr_code
    ,distribution_id
    ,delivery_method
    ,rng_start_dt
    , rng_end_dt
    FROM
        dwd.dwd_yjx_order_zip
    WHERE dt="2021-01-01") zip
    LEFT JOIN
        (
        SELECT
            id
            ,create_time
            ,update_time
            ,can_return
            ,cancel_reason
            ,client_type
            ,complete_time
            ,consignee_address_id_path
            ,consignee_address_path
            ,consignee_detail
            ,consignee_mobile
            ,consignee_name
            ,deliver_status
            ,discount_price
            ,flow_price
            ,freight_price
            ,goods_num
            ,goods_price
            ,logistics_code
            ,logistics_name
            ,logistics_no
            ,logistics_time
            ,member_id
            ,need_receipt
            ,order_status
            ,order_type
            ,parent_order_sn
            ,pay_order_no
            ,pay_status
            ,payment_method
            ,payment_time
            ,price_detail
            ,promotion_id
            ,remark
            ,store_id
            ,sn
            ,trade_sn
            ,weight
            ,qr_code
            ,distribution_id
            ,delivery_method
        FROM
        ods.ods_yjx_order_inc
        WHERE dt="2021-01-02"
        ) inc
    ON zip.sn = inc.sn
    --3.整合两个数据结果
    UNION ALL
    --2.对今天的所有增量数据生成新的拉链区间
    SELECT id
        ,create_time
        ,update_time
        ,can_return
        ,cancel_reason
        ,client_type
        ,complete_time
        ,consignee_address_id_path
        ,consignee_address_path
        ,consignee_detail
        ,consignee_mobile
        ,consignee_name
        ,deliver_status
        ,discount_price
        ,flow_price
        ,freight_price
        ,goods_num
        ,goods_price
        ,logistics_code
        ,logistics_name
        ,logistics_no
        ,logistics_time
        ,member_id
        ,need_receipt
        ,order_status
        ,order_type
        ,parent_order_sn
        ,pay_order_no
        ,pay_status
        ,payment_method
        ,payment_time
        ,price_detail
        ,promotion_id
        ,remark
        ,store_id
        ,sn
        ,trade_sn
        ,weight
        ,qr_code
        ,distribution_id
        ,delivery_method
           ,"2021-01-02" rng_start_dt
            ,"9999-12-31" rng_end_dt
    FROM
        ods.ods_yjx_order_inc
    WHERE dt="2021-01-02";
