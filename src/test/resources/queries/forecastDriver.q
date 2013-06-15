DROP TABLE employees;

-- data setup
CREATE TABLE employees( 
headcount DOUBLE, 
cost DOUBLE, 
name STRING, 
dat  DOUBLE 
);

LOAD DATA LOCAL INPATH '/media/MyPassport/hadoop/hive-ptfs/hive-ptfs/data/ptf_employees.txt' overwrite into table employees;

add jar file:///media/MyPassport/hadoop/hive-ptfs/hive-ptfs/target/hive-ptfs-0.0.1-SNAPSHOT.jar;

create temporary function forecastDriver as 'org.apache.hadoop.hive.ql.ptfs.ForecastDriver$ForecastDriverResolver';

select name, headcount, dat, cost, effect 
from forecastDriver(on employees  
partition by dat  
order by dat 
arg1('2012-06-02 00:00:00'), arg1(headcount), arg1(dat), arg1(cost)  
);