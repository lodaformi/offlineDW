create table dq.dq_dwd_app_event_detail_ssp(
    dt string,
    origin_sess_cnt bigint,
    split_sess_cnt bigint,
    origin_splited_cnt bigint,
    origin_not_splited_cnt bigint
)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dq/dq_dwd_app_event_detail_ssp';

INSERT INTO TABLE dq.dq_dwd_app_event_detail_ssp
SELECT
  "2021-02-01" dt,
  count(sessionid) origin_sess_cnt,
  sum(split_sess_cnt) as split_sess_cnt,
  count(`if`(split_sess_cnt>1,1,null)) origin_splited_cnt,
  count(`if`(split_sess_cnt=1,1,null)) origin_not_splited_cnt
FROM
    (SELECT
        sessionid,
        --一个session在30分钟内可能有多个操作，会分成多个相同的splitedsessionid，要去重
        count(distinct splitedsessionid) split_sess_cnt
    FROM
        dwd.dwd_app_event_detail
    WHERE dt="2021-02-01"
    GROUP BY sessionid) t
;

