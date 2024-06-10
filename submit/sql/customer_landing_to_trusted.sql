CREATE EXTERNAL TABLE IF NOT EXISTS `cuonglt`.`customer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cuonglt-lake-house/customer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Customer Landing to Trusted', 
  'CreatedByJobRun'='jr_95e5374c57729da5e0b04474542fec67a4d130b14f09e2ef0f119625236ce370', 
  'classification'='json');