show databases ;

drop table if exists test.t_student;
CREATE TABLE IF NOT EXISTS test.t_student (
    sno int,
    sname string
) PARTITIONED BY (grade int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "/part/";

load data inpath "/loda/s1.txt" into table test.t_student partition (grade=1);
load data inpath "/loda/s2.txt" into table test.t_student partition (grade=2);


SELECT * FROM test.t_student;