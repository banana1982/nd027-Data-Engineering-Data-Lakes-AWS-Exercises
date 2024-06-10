import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1718030510425 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1718030510425")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1718030537138 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="accelerometer_trusted", transformation_ctx="AccelerometerLanding_node1718030537138")

# Script generated for node Join
Join_node1718030581389 = Join.apply(frame1=CustomerTrusted_node1718030510425, frame2=AccelerometerLanding_node1718030537138, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1718030581389")

# Script generated for node Drop Fields
DropFields_node1718030820015 = DropFields.apply(frame=Join_node1718030581389, paths=["`.serialnumber`", "z", "`.birthday`", "`.sharewithpublicasofdate`", "`.sharewithresearchasofdate`", "`.registrationdate`", "`.customername`", "user", "y", "x", "timestamp", "`.lastupdatedate`", "`.sharewithfriendsasofdate`"], transformation_ctx="DropFields_node1718030820015")

# Script generated for node Drop Duplicates
DropDuplicates_node1718031084886 =  DynamicFrame.fromDF(DropFields_node1718030820015.toDF().dropDuplicates(["customername"]), glueContext, "DropDuplicates_node1718031084886")

# Script generated for node Customer Curated
CustomerCurated_node1718031108321 = glueContext.getSink(path="s3://cuonglt-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1718031108321")
CustomerCurated_node1718031108321.setCatalogInfo(catalogDatabase="cuonglt",catalogTableName="customer_curated")
CustomerCurated_node1718031108321.setFormat("json")
CustomerCurated_node1718031108321.writeFrame(DropDuplicates_node1718031084886)
job.commit()