drop table dwd.dwd_app_event_detail;
create external table dwd.dwd_app_event_detail
(
    account string,
    appId string,
    appVersion string,
    carrier string,
    deviceId string,
    eventId string,
    ip string,
    latitude double,
    longitude double,
    netType string,
    osName string,
    osVersion string,
    properties map<string,string>,
    resolution string,
    sessionId string,
    `timestamp` string,
    splitedSessionId string,
    filledAccount string,
    province string,
    city string,
    region string,
    guid bigint,
    isnew int
)
partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_app_event_detail'
tblproperties("parquet.compress" = "snappy");

select count(1),guid from dwd.dwd_app_event_detail
where dt='2021-02-01' group by guid;

SELECT * FROM dwd.dwd_app_event_detail WHERE dt="2021-02-05"