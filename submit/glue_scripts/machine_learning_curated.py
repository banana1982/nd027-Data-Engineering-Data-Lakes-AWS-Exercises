import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1718034207748 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1718034207748")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1718034155408 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1718034155408")

# Script generated for node SQL Query
SqlQuery3868 = '''
select distinct(stt.sensorreadingtime)
       ,stt.serialnumber
       ,stt.distancefromobject
       ,act.user
       ,act.x
       ,act.y
       ,act.z
from stt join act on stt.sensorreadingtime = act.timestamp
'''
SQLQuery_node1718034239654 = sparkSqlQuery(glueContext, query = SqlQuery3868, mapping = {"stt":StepTrainerTrusted_node1718034155408, "act":AccelerometerTrusted_node1718034207748}, transformation_ctx = "SQLQuery_node1718034239654")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1718034864331 = glueContext.getSink(path="s3://cuonglt-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1718034864331")
MachineLearningCurated_node1718034864331.setCatalogInfo(catalogDatabase="cuonglt",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1718034864331.setFormat("json")
MachineLearningCurated_node1718034864331.writeFrame(SQLQuery_node1718034239654)
job.commit()