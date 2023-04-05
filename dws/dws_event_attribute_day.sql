drop table dws.dws_event_attribute_day;
create external table dws.dws_event_attribute_day
(
    model STRING COMMENT '分析模型'
    ,strategy STRING COMMENT '归因分析类型'
    ,guid BIGINT COMMENT '用户唯一ID'
    ,dest_event STRING COMMENT '目标事件'
    ,attr_event STRING COMMENT '待归因事件'
    ,weight DOUBLE COMMENT '权重'
)PARTITIONED BY (dt STRING)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_event_attribute_day'
TBLPROPERTIES ('parquet.compress' = 'snappy');