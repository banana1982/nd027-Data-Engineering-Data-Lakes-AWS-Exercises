import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1718029826523 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://cuonglt-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1718029826523")

# Script generated for node PrivacyFilter
PrivacyFilter_node1718029886238 = Filter.apply(frame=CustomerLanding_node1718029826523, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1718029886238")

# Script generated for node Customer Trusted
CustomerTrusted_node1718029910507 = glueContext.getSink(path="s3://cuonglt-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1718029910507")
CustomerTrusted_node1718029910507.setCatalogInfo(catalogDatabase="cuonglt",catalogTableName="customer_trusted")
CustomerTrusted_node1718029910507.setFormat("json")
CustomerTrusted_node1718029910507.writeFrame(PrivacyFilter_node1718029886238)
job.commit()