drop table dws.dws_app_ld_compstep;
create external table dws.dws_app_ld_compstep
(
    guid BIGINT COMMENT '用户唯一ID',
    funnel_model STRING COMMENT '漏斗模型名称',
    eventid STRING COMMENT '最终事件名称',
    max_step INT COMMENT '最大步骤'
)PARTITIONED BY (dt STRING)
STORED AS parquet
LOCATION 'hdfs://hdfs-yjx/yjx/app/dws/dws_app_ld_compstep'
TBLPROPERTIES ('parquet.compress' = 'snappy');

-- select eventid from dwd.dwd_app_event_detail group by eventid
-- 电商行为数据事件字段有：
-- ColumnClick/adClick/adShow/addCart/categoryClick/collect/fetchCoupon
-- /goodsView/launch/orderComplaint/pageView/promotionClick/promotionShow
-- /revoke/search/share/signIn/submitOrder/thumbUp
WITH tmp AS (
    SELECT
    guid,
    --将数组中的元素使用逗号拼起来
    concat_ws(",",sort_array(collect_list(concat_ws("_", `timestamp`, eventid)))) event_seq
    FROM
    dwd.dwd_app_event_detail
    WHERE dt = "2021-02-01" AND
          (
              (eventid = "pageView") or (eventid = "addCart") or (eventid = "submitOrder")
          )
--           eventid in ('pageView' or "addCart" or "submitOrder"   )
    GROUP BY guid
)
INSERT INTO TABLE dws.dws_app_ld_compstep PARTITION (dt="2021-02-01")
SELECT
       guid,
       funnel_model,
       eventid,
       max_step
FROM
  (
    SELECT
            guid,
           "下单漏斗模型" funnel_model,
           case
               when regexp_extract(event_seq,".*?(pageView).*?(addCart).*?(submitOrder).*?",3) = "submitOrder" then "submitOrder"
               when regexp_extract(event_seq,".*?(pageView).*?(addCart).*?",2) = "addCart" then "addCart"
               when regexp_extract(event_seq,".*?(pageView).*?",1) = "pageView" then "pageView"
            end eventid,
           case
               when regexp_extract(event_seq,".*?(pageView).*?(addCart).*?(submitOrder).*?",3) = "submitOrder" then 3
               when regexp_extract(event_seq,".*?(pageView).*?(addCart).*?",2) = "addCart" then 2
               when regexp_extract(event_seq,".*?(pageView).*?",1) = "pageView" then 1
               else 0
            end max_step
    FROM tmp
    ) a
-- 有些用户没有触发下单漏斗模型中的事件，在正则匹配时不会匹配到
-- 加上限制条件，将这些数据过滤掉
WHERE max_step > 0;

