drop table if exists ads.ads_app_ld_conversion_rate;
create external table ads.ads_app_ld_conversion_rate
(
    funnel_model STRING COMMENT '漏斗模型名称',
    step INT COMMENT '步骤',
    rate DOUBLE COMMENT '转化率',
    step_cnt BIGINT COMMENT '步骤人数'
)PARTITIONED BY (dt STRING)
STORED AS parquet
LOCATION 'hdfs://hdfs-yjx/yjx/app/ads/ads_app_ld_conversion_rate';



WITH tmp AS (
    SELECT
           funnel_model,
           max_step step,
           count(max_step) cnt
    FROM
        dws.dws_app_ld_compstep
    WHERE dt="2021-02-01"
    GROUP BY funnel_model, max_step
)
INSERT INTO TABLE ads.ads_app_ld_conversion_rate PARTITION (dt="2021-02-01")
SELECT
            funnel_model,
           step,
            -- max(step_cnt) 使用开窗计算
            step_cnt/max(step_cnt) over (partition by funnel_model) rate,
            step_cnt
FROM
     (
        SELECT
            funnel_model,
            step,
            sum(cnt) over (partition by funnel_model order by step rows between current row and unbounded following) step_cnt
        FROM tmp
    ) a;