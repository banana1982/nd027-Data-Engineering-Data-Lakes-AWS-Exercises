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

# Script generated for node Customer Curated
CustomerCurated_node1718031800720 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="customer_curated", transformation_ctx="CustomerCurated_node1718031800720")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1718031839694 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://cuonglt-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1718031839694")

# Script generated for node SQL Query
SqlQuery3709 = '''
SELECT distinct(stl.sensorreadingtime)
      ,stl.serialnumber
      ,stl.distancefromobject
FROM stl
INNER JOIN cc ON cc.serialnumber = stl.serialnumber
'''
SQLQuery_node1718032471802 = sparkSqlQuery(glueContext, query = SqlQuery3709, mapping = {"cc":CustomerCurated_node1718031800720, "stl":StepTrainerLanding_node1718031839694}, transformation_ctx = "SQLQuery_node1718032471802")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1718032643006 = glueContext.getSink(path="s3://cuonglt-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1718032643006")
StepTrainerTrusted_node1718032643006.setCatalogInfo(catalogDatabase="cuonglt",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1718032643006.setFormat("json")
StepTrainerTrusted_node1718032643006.writeFrame(SQLQuery_node1718032471802)
job.commit()