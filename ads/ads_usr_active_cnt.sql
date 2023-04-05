drop table if exists ads.ads_usr_active_cnt;
create external table ads.ads_usr_active_cnt(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count` bigint COMMENT '当周用户数量',
    `mn_count` bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃用户数'
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_uv_count';


SELECT * FROM dws.dws_app_user_ctnu_rng where dt="2021-02-02" and rng_end_dt = "9999-12-31"

INSERT INTO TABLE ads.ads_usr_active_cnt
SELECT
        dayTable.dt,
       day_count,
       wk_count,
       mn_count,
       `if`(date_sub(next_day(dayTable.dt, 'MO'),1)=dayTable.dt, "Y", "N") is_weekend,
       `if`(last_day(dayTable.dt) = dayTable.dt, "Y", "N" ) is_monthend
FROM
(
SELECT
    "2021-02-02" dt,
    count(guid) day_count
FROM
    dws.dws_app_user_ctnu_rng
where dt="2021-02-02" and rng_end_dt = "9999-12-31") dayTable
join (
    SELECT
        "2021-02-02" dt,
        count(distinct guid) wk_count
    FROM
        dws.dws_app_user_ctnu_rng
    WHERE  (rng_end_dt >= date_sub(next_day("2021-02-02","MO"),7) and rng_end_dt <= dt and dt="2021-02-02")
        OR (rng_end_dt = "9999-12-31" and  dt="2021-02-02")
) wkTable
join (
    SELECT
       "2021-02-02" dt,
        count(distinct guid) mn_count
    FROM
        dws.dws_app_user_ctnu_rng
    WHERE (dt="2021-02-02" and date_format(dt, "yyyy-MM") = date_format(rng_end_dt, "yyyy-MM"))
    OR (rng_end_dt = "9999-12-31" and  dt="2021-02-02")
) mnTable
on dayTable.dt = wkTable.dt and wkTable.dt = mnTable.dt;