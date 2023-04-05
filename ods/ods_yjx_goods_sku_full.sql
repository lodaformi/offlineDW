drop table ods.ods_yjx_goods_sku_full;
create external table IF NOT EXISTS ods.ods_yjx_goods_sku_full(
    id bigint comment '商品skuID',
    create_time String comment '创建时间',
    delete_flag String comment '删除标志 true/false 删除/未删除',
    auth_message String comment '审核信息',
    brand_id String comment '品牌ID',
    category_path String comment '分类路径',
    cost String comment '成本价格',
    freight_payer String comment '运费承担者',
    goods_id String comment '商品ID',
    goods_name String comment '商品名称',
    goods_unit String comment '计量单位',
    goods_video String comment '商品视频',
    intro String comment '商品详情',
    is_auth String comment '审核状态',
    is_promotion String comment '是否是促销商品',
    promotion_price String comment '促销价',
    market_enable String comment '上架状态',
    mobile_intro String comment '商品移动端详情',
    original String comment '原图路径',
    price String comment '商品价格',
    sales_model String comment '销售模式',
    self_operated String comment '是否自营',
    store_id String comment '店铺ID',
    selling_point String comment '卖点',
    sn String comment '商品编号',
    specs String comment '规格json',
    simple_specs String comment '规格信息',
    store_category_path String comment '店铺分类路径',
    goods_type String comment '商品类别',
    weight String comment '重量',
    under_message String comment '下架原因'
)partitioned by(dt String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_goods_sku_full';

msck repair table ods.ods_yjx_goods_sku_full;