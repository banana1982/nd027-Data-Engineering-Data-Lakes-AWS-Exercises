import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1718030217485 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://cuonglt-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1718030217485")

# Script generated for node Customer Trusted
CustomerTrusted_node1718030186531 = glueContext.create_dynamic_frame.from_catalog(database="cuonglt", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1718030186531")

# Script generated for node Join
Join_node1718030322219 = Join.apply(frame1=CustomerTrusted_node1718030186531, frame2=AccelerometerLanding_node1718030217485, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1718030322219")

# Script generated for node Drop Fields
DropFields_node1718030340197 = DropFields.apply(frame=Join_node1718030322219, paths=["serialnumber", "sharewithpublicasofdate", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "sharewithfriendsasofdate", "email", "lastupdatedate", "phone"], transformation_ctx="DropFields_node1718030340197")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1718030393829 = glueContext.getSink(path="s3://cuonglt-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1718030393829")
AccelerometerTrusted_node1718030393829.setCatalogInfo(catalogDatabase="cuonglt",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1718030393829.setFormat("json")
AccelerometerTrusted_node1718030393829.writeFrame(DropFields_node1718030340197)
job.commit()