--多维流量分析cube建表
create external table ads.ads_app_tfc_cube(
     appid           string
    ,appversion         string
    ,carrier        string
    ,nettype        string
    ,osname         string
    ,province       string
    ,city           string
    ,region         string
    ,isnew              int
    ,enter_page_id      string
    ,exit_page_id       string
    ,is_jumpout         string
    ,pv_cnt         bigint
    ,uv_cnt         bigint
    ,ses_cnt        bigint
    ,acc_tml        bigint
    ,avg_ses_tml        bigint
    ,ip_cnt             bigint
    ,jpt_ses_cnt    bigint
)
partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_app_tfc_cube'
tblproperties(
'parquet.compress'='snappy'
);

with tmp as (
SELECT
     nvl(account, "UNKNOWN")                ,account
    ,nvl(appid, "UNKNOWN")              ,appid
    ,nvl(appversion, "UNKNOWN")             ,appversion
    ,nvl(carrier, "UNKNOWN")                ,carrier
    ,nvl(deviceid, "UNKNOWN")               ,deviceid
    ,nvl(eventid, "UNKNOWN")                ,eventid
    ,nvl(ip, "UNKNOWN")                     ,ip
    ,nvl(latitude, "UNKNOWN")               ,latitude
    ,nvl(longitude, "UNKNOWN")              ,longitude
    ,nvl(nettype, "UNKNOWN")                ,nettype
    ,nvl(osname, "UNKNOWN")             ,osname
    ,nvl(osversion, "UNKNOWN")              ,osversion
    ,nvl(resolution, "UNKNOWN")             ,resolution
    ,nvl(`timestamp`, "UNKNOWN")                ,`timestamp`
    ,nvl(filledaccount, "UNKNOWN")              ,filledaccount
    ,nvl(province, "UNKNOWN")               ,province
    ,nvl(city, "UNKNOWN")               ,city
    ,nvl(region, "UNKNOWN")             ,region
    ,nvl(guid, "UNKNOWN")               ,guid
    ,nvl(isnew, "UNKNOWN")              ,isnew
    ,nvl(page_acc_tml, "UNKNOWN")               ,page_acc_tml
    ,nvl(splitedSessionId, "UNKNOWN")               ,splitedSessionId
    ,nvl(enter_page_id, "UNKNOWN")              ,enter_page_id
    ,nvl(exit_page_id, "UNKNOWN")               ,exit_page_id
    ,nvl(start_time, "UNKNOWN")             ,start_time
    ,nvl(end_time, "UNKNOWN")               ,end_time
    ,nvl(is_jumpout, "UNKNOWN")             ,is_jumpout
    ,nvl(pv_cnt, "UNKNOWN")                 ,pv_cnt
FROM
    dws.dws_app_tfc_topic
    where dt='2021-02-01'
)
INSERT INTO TABLE ads.ads_app_tfc_cube partition (dt='2021-02-01')
SELECT
    appid,
    appversion,
    carrier,
    nettype,
    osname,
    province,
    city,
    region,
    isnew,
    enter_page_id,
    exit_page_id,
    is_jumpout,
   count(1) pv_cnt,
   count(distinct guid) uv_cnt,
   count(distinct splitedsessionid) ses_cnt,
   sum(page_acc_tml) acc_tml,
    -- 平均访问时长针对的是会话，一个会话中可能有很多数据，
    -- 如果直接使用avg，会话的次数是跟数据的条数是一样的，这样不对，不能直接使用avg(page_acc_tml)函数算
   sum(page_acc_tml)/count(distinct(splitedsessionid)) avg_ses_tml,
    count(distinct ip) ip_cnt,
   count(distinct `if`(is_jumpout='Y', splitedsessionid, null)) jpt_ses_cnt
FROM
    tmp
group by
    appid, appversion, carrier, nettype, osname,
    province, city, region, isnew, enter_page_id, exit_page_id, is_jumpout
grouping sets (
    (appid),
    (appid, appversion),
    (osname),
    (carrier, nettype),
    (province),
    (province, city),
    (province, city, region),
    (isnew),
    (is_jumpout),
    (province, isnew),
    (enter_page_id),
    (exit_page_id)
);

SELECT * FROM ads.ads_app_tfc_cube WHERE city = "UNKNOWN";

SELECT nvl('NULL', 'UNKNOWN');

SELECT coalesce(NULL, "HHH", 'LISI', "RRR", 'WANGSU');

-- 当coalescen内部的参数全部为null时，返回是空即<null>
SELECT coalesce(NULL, null, NULL, NULL, NULL);

SELECT
*
FROM
ads.ads_app_tfc_cube
where dt="2021-02-01"
  and
    isnew is not null
and coalesce(
   appid,
    appversion,
    carrier,
    nettype,
    osname,
    province,
    city,
    region,
    enter_page_id,
    exit_page_id,
    is_jumpout
    ) is null;


SELECT
*
FROM
ads.ads_app_tfc_cube
-- WHERE ip_cnt is null and carrier is null ....
where dt="2021-02-01"
  and
    province is not null
and coalesce(
   appid,
    appversion,
    carrier,
    nettype,
    osname,
    city,
    region,
    isnew,
    enter_page_id,
    exit_page_id,
    is_jumpout
    ) is null