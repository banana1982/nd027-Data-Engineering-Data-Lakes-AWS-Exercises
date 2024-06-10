CREATE EXTERNAL TABLE IF NOT EXISTS `cuonglt`.`customer_curated`(
  `serialnumber` string COMMENT 'from deserializer', 
  `.customername` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `.sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `.lastupdatedate` bigint COMMENT 'from deserializer', 
  `.birthday` string COMMENT 'from deserializer', 
  `.registrationdate` bigint COMMENT 'from deserializer', 
  `.serialnumber` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `.sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer', 
  `.sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cuonglt-lake-house/customer/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Customer Trusted to Curated', 
  'CreatedByJobRun'='jr_3743cd3bde06d9dd456be96478583a5320a104c03b09e7ae31b15c34ce61aed4', 
  'classification'='json')