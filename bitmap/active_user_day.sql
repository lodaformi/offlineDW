create database test;
drop table if exists test.active_user_day;
create external table test.active_user_day(
    dt      string,
    guid    string
)
row format delimited fields terminated by ',';

load data inpath '/root/dau_bm.txt' into table test.active_user_day;

-- 位图使用规则
-- 脚标按照从最左边开始为今天日期，往右依次为距当天1日前、距当天2日前，以此类推，
-- 当然也可以从右边为今天日期，只是在具体使用时，算法会相应调整，本质都一样
-- 计算（所有/某一个）用户当前（时间）登录的状态值（一个月）
-- 思路：二进制转换为十进制，相应位乘以2的权值，求和
SELECT
    guid,
       -- 有聚合函数，所以要group by
    sum(pow(2, datediff(dt,date_sub("2021-01-21",30)))) bitmap
FROM
    test.active_user_day
-- 大日期在前，计算不出数据？？？
WHERE dt between date_sub("2021-01-21",30) AND "2021-01-21"
GROUP BY guid;

-- 当新数据到来时，要实现数据的滚动
-- create table test.user_active_bitmap
-- 本质上是：前一日的bitmap表与今日的日活用户做full join
-- 如果前一日用户登陆并且今日没登陆，则bitmap值为右移一位（相当于除2）
-- 如果前一日用户登陆并且今日登陆，则bitmap值为右移一位（相当于除2）+pow(2,29)
-- 如果前一日用户未登陆并且今日登陆，则bitmap值为pow(2,29)
WITH tmp AS (
    SELECT
        guid
        ,sum(pow(2, datediff(dt, date_sub("2021-01-21", 30) ))) bitmap
    FROM
        test.active_user_day
    WHERE dt BETWEEN date_sub("2021-01-21", 30) AND "2021-01-21"
    GROUP BY guid
 )
INSERT OVERWRITE table test.user_active_bitmap
SELECT
    nvl(tmp.guid, dau.guid) as guid,
    cast(case
        when tmp.guid is not null and dau.guid is null then bitmap/2
        when tmp.guid is not null and dau.guid is not null then bitmap/2+pow(2,29)
        when tmp.guid is null and dau.guid is not null then pow(2,29)
    end as int ) bitmap
FROM
tmp
FULL JOIN
    (
    SELECT
        guid
    FROM
        test.active_user_day WHERE dt = "2021-01-22"
    ) dau
on tmp.guid = dau.guid;



-- 需求1：求指定日期范围内连续活跃的人，18-20号连续登陆
-- 需要参考值，参考值怎么算，21号的bitmap减去18的bitmap，
-- 如果本身的bitmap与参考值与运算后相等，则表示是连续登陆
--   000000 000100 000000 000000 000000
--   000000 000000 100000 000000 000000
--  -
--   000000 000011 100000 000000 000000

SELECT
    guid
FROM test.user_active_bitmap
WHERE pow(2,datediff("2021-10-21", date_sub("2021-01-18",30))) -
      pow(2,datediff("2021-01-18", date_sub("2021-01-18",30))) | bitmap = bitmap;









-- 需求2：求指定日期范围内每个人的活跃天数，
-- 解题思路：
-- 就是求指定区间1的个数，将数字转化为字符串处理，将十进制数转使用bin()转为二进制数，
-- 不够30位的二进制数，使用lpad将在左边补0，reverse()反转，因为substr是从左到右切割的
-- 计算出开始切割的位置，计算出切割的长度，注意：日期相减后要+1,
-- 将切割后字符串中的0替换为空，此时剩下的就是1，使用length统计长度即可。
SELECT
    guid,
    length(replace(substr(reverse(lpad(bin(bitmap),30,"0")),
        -- 从哪里开始截取
        datediff("2021-01-18", date_sub("2021-01-22",30)),
        -- 截取多少位
        datediff("2021-01-20", "2021-01-18")+1),"0", "")) act_cnt
FROM test.user_active_bitmap;