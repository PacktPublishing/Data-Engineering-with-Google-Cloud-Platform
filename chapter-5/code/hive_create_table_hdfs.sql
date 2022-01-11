CREATE EXTERNAL TABLE simple_table(
    col_1 STRING,
    col_2 STRING,
    col_3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 'data/simple_file'
TBLPROPERTIES ("skip.header.line.count"="1")
;
