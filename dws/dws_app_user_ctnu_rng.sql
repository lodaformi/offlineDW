drop table dws.dws_app_user_ctnu_rng;
create table dws.dws_app_user_ctnu_rng(
    guid            bigint,
    first_acc_dt    string,
    rng_stat_dt     string,
    rng_end_dt      string
)
partitioned by (dt string)
row format delimited fields terminated by ',';

load data inpath '/root/rng.csv' into table dws.dws_app_user_ctnu_rng partition(dt='2021-08-09');

-- WITH tmp1  AS (
--     SELECT
--         guid
--         ,first_acc_dt
--         ,rng_stat_dt
--         ,rng_end_dt
--     FROM
--     dws.dws_app_user_ctnu_rng
--     WHERE dt="2021-08-09" AND rng_end_dt != "9999-12-31"
-- ),
-- tmp2 AS (
--     SELECT
--        nvl(rng.guid, dau.guid) guid
--         ,nvl(rng.first_acc_dt, dau.first_acc_dt) first_acc_dt
--         ,nvl(rng.rng_stat_dt, "2021-08-10") rng_stat_dt
--         ,`if`(rng.guid is not null and dau.guid is null, date_sub("2021-08-10",1), "9999-12-31") rng_end_dt
--     FROM
--         ( SELECT
--             guid
--             ,first_acc_dt
--             ,rng_stat_dt
--             ,rng_end_dt
--         FROM
--         dws.dws_app_user_ctnu_rng
--         WHERE dt="2021-08-09" AND rng_end_dt = "9999-12-31"
--         ) rng
--         FULL JOIN
--         (
--             SELECT
--                 guid
--                 ,isnew
--                 ,first_acc_dt
--             FROM
--             dwd.dwd_app_user_dau
--             WHERE dt = "2021-08-10"
--         ) dau
--     ON rng.guid = dau.guid
-- )
INSERT INTO TABLE dws.dws_app_user_ctnu_rng PARTITION (dt="2021-08-10")
SELECT
    guid
    ,first_acc_dt
    ,rng_stat_dt
    ,rng_end_dt
FROM
(
   SELECT
         nvl(rng.guid, dau.guid) guid
        ,nvl(rng.first_acc_dt, dau.first_acc_dt) first_acc_dt
        ,nvl(rng.rng_stat_dt, "2021-08-10") rng_stat_dt
        -- 这一条判断结果能解决两种情况
        -- 字面上的语句精确匹配T-1出现，但是T没出现的用户，表示该用户T日没有活跃，end_time赋为T-1
        -- 第二种情况，T-1没有出现，T出现，end_time赋值9999-12-31
        -- 第三种情况，T-1出现，T出现，end_time不变，不用处理
        ,`if`(rng.guid is not null and dau.guid is null, date_sub("2021-08-10",1), "9999-12-31") rng_end_dt
    FROM
        ( SELECT
            guid
            ,first_acc_dt
            ,rng_stat_dt
            ,rng_end_dt
        FROM
        dws.dws_app_user_ctnu_rng
        WHERE dt="2021-08-09" AND rng_end_dt = "9999-12-31"
        ) rng
        FULL JOIN
        (
            SELECT
                guid
                ,isnew
                ,first_acc_dt
            FROM
            dwd.dwd_app_user_dau
            WHERE dt = "2021-08-10"
        ) dau
    ON rng.guid = dau.guid
    )as abc  union all (
    SELECT
        guid
        ,first_acc_dt
        ,rng_stat_dt
        ,rng_end_dt
    FROM
    dws.dws_app_user_ctnu_rng
    WHERE dt="2021-08-09" AND rng_end_dt != "9999-12-31"
);


WITH tmp1  AS (
    SELECT
        guid
        ,first_acc_dt
        ,rng_stat_dt
        ,rng_end_dt
    FROM
    dws.dws_app_user_ctnu_rng
    WHERE dt="2021-08-09" AND rng_end_dt != "9999-12-31"
),
tmp2 AS (
    SELECT
       nvl(rng.guid, dau.guid) guid
        ,nvl(rng.first_acc_dt, dau.first_acc_dt) first_acc_dt
        ,nvl(rng.rng_stat_dt, "2021-08-10") rng_stat_dt
        -- 这一条判断结果能解决两种情况
        -- 字面上的语句精确匹配T-1出现，但是T没出现的用户，表示该用户T日没有活跃，end_time赋为T-1
        -- 第二种情况，T-1没有出现，T出现，end_time赋值9999-12-31
        -- 第三种情况，T-1出现，T出现，end_time赋值9999-12-31
        ,`if`(rng.guid is not null and dau.guid is null, date_sub("2021-08-10",1), "9999-12-31") rng_end_dt
    FROM
        ( SELECT
            guid
            ,first_acc_dt
            ,rng_stat_dt
            ,rng_end_dt
        FROM
            dws.dws_app_user_ctnu_rng
        WHERE dt="2021-08-09" AND rng_end_dt = "9999-12-31"
        ) rng
        FULL JOIN
        (
            SELECT
                guid
                ,isnew
                ,first_acc_dt
            FROM
                dwd.dwd_app_user_dau
            WHERE dt = "2021-08-10"
        ) dau
    ON rng.guid = dau.guid
)
INSERT INTO TABLE dws.dws_app_user_ctnu_rng PARTITION (dt="2021-08-10")
SELECT
    guid
    ,first_acc_dt
    ,rng_stat_dt
    ,rng_end_dt
FROM
tmp1  union all
SELECT
    guid
    ,first_acc_dt
    ,rng_stat_dt
    ,rng_end_dt
    from
tmp2;


with tmp1 as (
select
guid
,first_acc_dt
,rng_stat_dt
,rng_end_dt
from dws.dws_app_user_ctnu_rng
where dt='2021-08-09' and rng_end_dt !='9999-12-31'
),
tmp2 as (
select
nvl(his.guid,cur.guid) as guid
,nvl(his.first_acc_dt,cur.first_acc_dt) as first_acc_dt
,nvl(his.rng_stat_dt,'2021-08-10') as rng_start_dt
,if(his.guid is not null and cur.guid is null, date_sub('2021-08-10',1),'9999-
12-31') as rng_end_dt
from
(select
guid
,first_acc_dt
,rng_stat_dt
,rng_end_dt
from dws.dws_app_user_ctnu_rng
where dt='2021-08-09' and rng_end_dt ='9999-12-31') his
full join
(select * from dwd.dwd_app_user_dau where dt='2021-08-10') cur
on his.guid = cur.guid
)
insert into table dws.dws_app_user_ctnu_rng partition(dt='2021-08-10')
-- 为什么会报语法上的错误？？？
--org.apache.hadoop.hive.ql.parse.ParseException:line 31:29 cannot recognize input near 'tmp2' '<EOF>' '<EOF>' in statement
select * from tmp1 union all tmp2;


-------------------------------------------------------------------------------------------------------------
--真实数据
drop table if exists dws.dws_app_user_ctnu_rng;
create external table dws.dws_app_user_ctnu_rng(
    guid            bigint,
    first_acc_dt    string,
    rng_start_dt    string,
    rng_end_dt       string
)
partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dws/dws_app_user_ctnu_rng';

with tmp1 as (
        select
            guid
            ,first_acc_dt
            ,rng_start_dt
            ,rng_end_dt
        from dws.dws_app_user_ctnu_rng
        where dt='2021-02-04' and rng_end_dt !='9999-12-31'
    )
    ,tmp2 as (
    select
        nvl(pre.guid,cur.guid) as guid
        ,nvl(pre.first_acc_dt, cur.first_acc_dt) as first_acc_dt
        ,nvl(pre.rng_start_dt,'2021-02-05') as rng_start_dt
        ,if(pre.guid is not null and cur.guid is null, date_sub('2021-02-05',1),'9999-12-31') as rng_end_dt
    from
        (select
            guid
            ,first_acc_dt
            ,rng_start_dt
            ,rng_end_dt
        from dws.dws_app_user_ctnu_rng
        where dt='2021-02-04' and rng_end_dt = '9999-12-31') pre
    full join
        (select * from  dwd.dwd_app_user_dau where dt='2021-02-05') cur
    on pre.guid = cur.guid
    )
    insert overwrite table dws.dws_app_user_ctnu_rng partition(dt='2021-02-05')
    select
        guid,
        first_acc_dt,
        rng_start_dt,
        rng_end_dt
    from tmp1 union all
    select
    guid,
    first_acc_dt,
    rng_start_dt,
    rng_end_dt
    from tmp2;
