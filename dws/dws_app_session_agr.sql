drop table dws.dws_app_session_agr;
-- 建表：app端会话信息聚合表
CREATE external TABLE dws.dws_app_session_agr(
    guid                bigint,
    splitedsessionid    string,
    enter_page_id       string,
    exit_page_id        string,
    start_time          bigint,
    end_time            bigint,
    is_jumpout          string, --是否是跳出会话
    pv_cnt              bigint
)
partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_app_session_agr';

-- 会话信息聚合（会话信息维表）
INSERT OVERWRITE TABLE dws.dws_app_session_agr PARTITION (dt="2021-02-01")
SELECT
    guid,
    splitedSessionId,
    split(min(`if`(eventid='pageView', concat_ws("_", `timestamp`, eventid, properties['pageId']), '9')), "_")[2] enter_page_id, --进入页
    split(max(`if`(eventid='pageView', concat_ws("_", `timestamp`, eventid, properties['pageId']), '0')), "_")[2] exit_page_id, --跳出页
     min(`timestamp`) start_time,--开始时间
     max(`timestamp`) end_time,--结束时间
    -- 这条语句有问题，因为pageView可以访问不同的页面，判断的维度可以再细一点
    `if`(count(`if`(eventid='pageView', 1, null)) < 2, "Y", "N") is_jumpout, --是否是跳出会话
    -- `if`(count(`if`(eventid='pageView', properties['pageId'], null)) = 1, "Y", "N") is_jumpout2, --是否是跳出页
     count(`if`(eventid='pageView', 1, null)) pv_cnt --pv
FROM
     dwd.dwd_app_event_detail
WHERE dt = '2021-02-01'
GROUP BY guid, splitedSessionId;