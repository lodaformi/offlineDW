---------------
-- 模拟数据
drop table dwd.dwd_app_user_dau;
create table dwd.dwd_app_user_dau(
    guid            bigint,
    isnew           int,
    first_acc_dt    string
)
partitioned by (dt string)
row format delimited fields terminated by ','
stored as parquet;

load data inpath '/root/dau.csv' into table dwd.dwd_app_user_dau partition(dt='2021-08-10');
------------------------------------------
-- 真实数据
create external table dwd.dwd_app_user_dau(
    guid  bigint,
    isnew int,
    first_acc_dt string
)
partitioned by (dt string)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_app_user_dau';

INSERT OVERWRITE table dwd.dwd_app_user_dau partition(dt='2021-02-06')
select
    distinct a.guid,
    isnew isnew,
    if(isnew=1, dt, b.first_acc_dt) first_acc_dt
from
    dwd.dwd_app_event_detail a
left join
    dwd.dwd_app_user_register b
on a.guid = b.guid
where a.guid >= 1
    and dt = '2021-02-06';

SELECT * FROM dwd.dwd_app_user_dau WHERE dt="2021-02-05";

INSERT OVERWRITE table dwd.dwd_app_user_dau partition(dt='${cur_date}')
select
    distinct a.guid,
    isnew isnew,
    if(isnew=1, dt, b.first_acc_dt) first_acc_dt
from
    dwd.dwd_app_event_detail a
left join
    dwd.dwd_app_user_register b
on a.guid = b.guid
where a.guid >= 1
    and dt = '${cur_date}';