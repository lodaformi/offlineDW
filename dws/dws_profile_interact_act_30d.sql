-- 用户交互事件属性标签
DROP TABLE IF EXISTS dws.dws_profile_interact_act_30d;
create external table dws.dws_profile_interact_act_30d(
    guid int,
    thumbUp_num bigint,
    collect_num bigint,
    addCart_num bigint,
    share_num bigint
)
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_profile_interact_act_30d';


select
    eventid
from
dwd.dwd_app_event_detail where dt="2021-02-01"
group by eventid;

with tmp as (
    SELECT
        guid,
        str_to_map(concat_ws(",",collect_list(concat_ws(":", eventid, cast(cnt as string )))),",", ":") event_map
    FROM(
        SELECT
            guid,
           eventid,
           count(1) as cnt
        FROM dwd.dwd_app_event_detail
        WHERE dt="2021-02-01"
        GROUP BY guid,eventid
        ) a
    -- 为什么要group by？？？
    group by guid
 )
INSERT INTO TABLE dws.dws_profile_interact_act_30d
SELECT
        guid,
       nvl(event_map['thumbUp'],0) thumbUp_num,
       nvl(event_map['collect'],0) collect_num,
       nvl(event_map['addCart'],0) addCart_num,
       nvl(event_map['share'],0) share_num
FROM
tmp
;