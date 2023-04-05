drop table if exists dws.dws_user_ret_cnt;
create external table dws.dws_user_ret_cnt (
    dt string comment '计算日期',
    first_acc_dt string comment '首访日期',
    ret_days int comment '留存天数',
    ret_usr_cnt int comment '留存人数'
)
stored as parquet
location  'hdfs://hdfs-yjx/yjx/app/dws/dws_user_ret_cnt';

insert into table dws.dws_user_ret_cnt
select
    '2021-02-03' dt,
    first_acc_dt,
    `datediff`('2021-02-03', first_acc_dt) as ret_days,
    count(distinct  guid) as ret_usr_cnt
from
    dws.dws_app_user_ctnu_rng
where dt='2021-02-03' and datediff('2021-02-03',first_acc_dt) <= 30 and rng_end_dt='9999-12-31'
group by first_acc_dt;

SELECT * FROM dws.dws_app_user_ctnu_rng WHERE dt="2021-02-03";