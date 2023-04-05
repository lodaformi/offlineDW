-- 例子，手动计算省维度下的流量指标
CREATE DATABASE ads;
CREATE external TABLE ads.ads_province_tfc_1d(
    province string
    ,pv bigint
    ,uv bigint
    ,ses_cnt bigint
    ,ip_cnt bigint
    ,jpt_ses_cnt bigint
    ,acc_tml bigint
)
PARTITIONED BY(dt string)
STORED AS parquet
location 'hdfs://hdfs-yjx/yjx/app/ads/ads_province_tfc_1d';

INSERT OVERWRITE TABLE ads.ads_province_tfc_1d
select
    province,
    count(1) pv,
    count(distinct guid) uv,
    count(distinct splitedsessionid) ses_cnt,
    count(distinct ip) ip_cnt,
    count(distinct if(is_jumpout='Y',splitedsessionid, null)) jpt_ses_cnt,
    sum(page_acc_tml) acc_tml,
    '2021-02-01' dt
from
    dws.dws_app_tfc_topic
where dt='2021-02-01'
-- group by 有去重的作用
group by province;