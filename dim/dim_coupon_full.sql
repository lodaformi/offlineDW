DROP TABLE IF EXISTS dim.dim_coupon_full;
CREATE EXTERNAL TABLE dim.dim_coupon_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type_code` STRING COMMENT '购物券类型编码',
        --
    `coupon_type_name` STRING COMMENT '购物券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    --
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    --
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       STRING COMMENT '可以领取的开始日期',
    `end_time`         STRING COMMENT '可以领取的结束日期',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS parquet
    LOCATION 'hdfs://hdfs-yjx/yjx/app/dim/dim_coupon_full';


set hive.vectorized.execution.enabled = false;

insert into table dim.dim_coupon_full partition (dt='2021-01-01')
select
    id,
    coupon_name,
    coupon_type coupon_type_code,
    b.dic_name coupon_type_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201'   then concat('满',condition_amount,'元，减',benefit_amount,"元")
        when '3202'   then concat('满',condition_num,'件，打',10*(1-benefit_discount),"折")
        when '3203'   then concat('减',benefit_amount,"元")
        when '3204'   then concat('打',10*(1-benefit_discount),"折")
    end benefit_rule,
    create_time,
    range_type range_type_code,
    c.dic_name range_type_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
    from
    (
        select
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    from
        ods.ods_yjx_coupon_info_full
    where dt='2021-01-01'
        ) a
left join
    (
        select
        dic_code,
        dic_name
    from
        ods.ods_yjx_dic_full
    where dt='2021-01-01'
    and parent_code='32'
        ) b
on a.coupon_type = b.dic_code
left join
    (
    select
        dic_code,
        dic_name
    from
        ods.ods_yjx_dic_full
    where dt='2021-01-01'
    and parent_code='33'
        ) c
on a.range_type = c.dic_code;