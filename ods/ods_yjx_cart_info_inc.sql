DROP TABLE IF EXISTS ods.ods_yjx_cart_info_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_yjx_cart_info_inc
(
    `id` STRING COMMENT '编号',
    `member_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'sku_id',
    `cart_price` DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num` BIGINT COMMENT '数量',
    `img_url` STRING COMMENT '商品图片地址',
    `sku_name` STRING COMMENT 'sku名称 (冗余)',
    `is_checked` STRING COMMENT '是否被选中',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered` STRING COMMENT '是否已经下单',
    `order_time` STRING COMMENT '下单时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
NULL DEFINED AS ''
LOCATION 'hdfs://hdfs-yjx/yjx/app/ods/ods_yjx_cart_info_inc';

msck repair table ods.ods_yjx_cart_info_inc;

show partitions ods.ods_yjx_cart_info_inc