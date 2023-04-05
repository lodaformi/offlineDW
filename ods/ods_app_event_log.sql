show databases;
create database ods;

create external table if not exists ods.ods_app_event_log
(
    account         string,
    appId           string,
    appVersion      string,
    carrier         string,
    deviceId        string,
    eventId          string,
    ip              string,
    latitude        double,
    longitude       double,
    netType         string,
    osName          string,
    osVersion       string,
    properties      map<string,string>, -- 事件属性
    resolution      string, -- 分辨率
    sessionId       string, -- 会话id
    `timeStamp`     bigint -- 事件时间
)
partitioned by (dt string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location 'hdfs://hdfs-yjx/yjx/app/ods/ods_app_event_log'
;

select * from ods.ods_app_event_log;

-- 报错 Unable to move source hdfs://hdfs-yjx/yjx/app/ods/ods_app_event_log/dt=2021-02-01 to
-- destination hdfs://hdfs-yjx/yjx/app/ods/ods_app_event_log/dt=2021-02-01
load data inpath 'hdfs://hdfs-yjx/yjx/app/ods/ods_app_event_log/dt=2021-02-01'
into table ods.ods_app_event_log
partition(dt='2021-02-01');

alter table ods.ods_app_event_log add if not exists
partition(dt='2021-02-01');

msck repair table ods.ods_app_event_log;

show partitions ods.ods_app_event_log;