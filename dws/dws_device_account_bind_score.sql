create database dws;

drop table if exists dws.dws_device_account_bind_score;
create external table if not exists dws.dws_device_account_bind_score(
    deviceid string,
    account string,
    score double,
    last_login string
)partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="snappy");

show partitions dws.dws_device_account_bind_score;