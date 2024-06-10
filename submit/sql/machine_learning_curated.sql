CREATE EXTERNAL TABLE IF NOT EXISTS `cuonglt`.`machine_learning_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cuonglt-lake-house/step_trainer/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Machine Learning Curated', 
  'CreatedByJobRun'='jr_7f24f531d6ef1bf158aa730e270b945817d0da17fc8d746ab15de515dd04e413', 
  'classification'='json');