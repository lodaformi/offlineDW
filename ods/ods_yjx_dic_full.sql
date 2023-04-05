DROP TABLE IF EXISTS ods.ods_yjx_dic_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_yjx_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
	NULL DEFINED AS ''
    LOCATION 'hdfs://hdfs-yjx/yjx/app/ods/ods_dic_full';

msck repair table ods.ods_yjx_dic_full;