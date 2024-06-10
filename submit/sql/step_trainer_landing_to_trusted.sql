CREATE EXTERNAL TABLE IF NOT EXISTS `cuonglt`.`step_trainer_trusted` (
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cuonglt-lake-house/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing to Trusted', 
  'CreatedByJobRun'='jr_08e95de1ecbb92e18812b18d135c1fb4b178a1e5df39f3ff678bd2d178da4b41', 
  'classification'='json');