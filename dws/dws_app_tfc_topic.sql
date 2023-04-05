-- 建表：app端流量明细大宽表
-- dws : 数仓服务层（轻聚合，汇总表）
CREATE external TABLE dws.dws_app_tfc_topic(
    account             string
    ,appid              string
    ,appversion         string
    ,carrier            string
    ,deviceid           string
    ,eventid            string
    ,ip                 string
    ,latitude           double
    ,longitude          double
    ,nettype            string
    ,osname             string
    ,osversion          string
    ,properties         map<string,string>
    ,resolution         string
    ,`timestamp`        bigint
    ,filledaccount      string
    ,province           string
    ,city               string
    ,region             string
    ,guid               bigint
    ,isnew              int
    ,page_acc_tml       bigint        -- 页面访问时长
    ,splitedSessionId   string
    ,enter_page_id      string
    ,exit_page_id       string
    ,start_time         bigint
    ,end_time           bigint
    ,is_jumpout         string
    ,pv_cnt             bigint
)PARTITIONED BY(dt string)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_app_tfc_topic';

-- 第2步，补充计算每个页面的停留时长
WITH event as (
    SELECT
        account
        ,appid
        ,appversion
        ,carrier
        ,deviceid
        ,eventid
        ,ip
        ,latitude
        ,longitude
        ,nettype
        ,osname
        ,osversion
        ,properties
        ,resolution
        ,sessionid
        ,`timestamp`
        ,splitedsessionid
        ,filledaccount
        ,province
        ,city
        ,region
        ,guid
        ,isnew
        ,dt
    FROM dwd.dwd_app_event_detail
    WHERE dt='2021-02-01' and eventid='pageView'
),
     sess as (
         SELECT
             guid
            ,splitedsessionid
            ,enter_page_id
            ,exit_page_id
            ,start_time
            ,end_time
            ,is_jumpout
            ,pv_cnt
         FROM dws.dws_app_session_agr
         WHERE dt='2021-02-01'
     )
INSERT OVERWRITE TABLE dws.dws_app_tfc_topic PARTITION(dt='2021-02-01')
SELECT
      event.account
     ,event.appid
     ,event.appversion
     ,event.carrier
     ,event.deviceid
     ,event.eventid
     ,event.ip
     ,event.latitude
     ,event.longitude
     ,event.nettype
     ,event.osname
     ,event.osversion
     ,event.properties
     ,event.resolution
     ,event.`timestamp`
     ,event.filledaccount
     ,event.province
     ,event.city
     ,event.region
     ,event.guid
     ,event.isnew
     -- LEAD(COL, N, DEFAULT_VAL) ：往后第 N 行数据，没有数据的话用 DEFAULT_VAL 代替
     -- 最后一个页面的访问时长是0吗？？？
     ,lead(event.`timestamp`, 1, sess.end_time)
           over(partition by sess.splitedsessionid order by event.`timestamp`) - event.`timestamp`
        as page_acc_tml --页面停留时长
     ,event.splitedsessionid as splitedSessionid
     ,sess.enter_page_id
     ,sess.exit_page_id
     ,sess.start_time
     ,sess.end_time
     ,sess.is_jumpout
     ,sess.pv_cnt
FROM event LEFT JOIN sess
 ON event.splitedsessionid = sess.splitedsessionid;