CREATE EXTERNAL TABLE IF NOT EXISTS `cuonglt`.`accelerometer_landing` (
  `serialnumber` string COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cuonglt-lake-house/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing to Trusted', 
  'CreatedByJobRun'='jr_28fc81455fe01007ccd340028fc93fb6d5957db91214b6295fa4c432b6f8cf20', 
  'classification'='json');