drop table if exists test.student_s;
create table if not exists test.student_s(
    id varchar(4),
    user_name varchar(4),
    cn_score int,
    math_score int,
    en_score int
);
insert into test.student_s values
('1','张三',34,66,58),
('2','李四',60,88,68),
('3','王五',90,100,98),
('4','赵六',52,78,48),
('5','马七',89,100,99);

SELECT
    user_name,
       sc
FROM
test.student_s,
     -- 有语法错误？？？
lateral view explode(split(concat_ws(",", cn_score, math_score, en_score),",")) sss as sc;

SELECT
        user_name,
       explode(split(concat_ws(",",cn_score,math_score,en_score),","))
FROM
test.student_s