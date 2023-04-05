create external table dwd.dwd_app_user_register(
    guid            bigint,
    first_acc_dt    string
)
stored as parquet
location 'hdfs://hdfs-yjx/yjx/app/dwd/dwd_app_user_register';

insert into table dwd.dwd_app_user_register
select
    guid,
    dt
from
    dwd.dwd_app_event_detail
where
    isnew = 1
    and dt='2021-02-05'
    and guid is not null
group by guid,dt;