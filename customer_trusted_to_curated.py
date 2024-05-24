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

# Script generated for node accelerometer_landing
accelerometer_landing_node1716514553725 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://olaitans3bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1716514553725")

# Script generated for node customer_trusted
customer_trusted_node1716514431042 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://olaitans3bucket/Customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1716514431042")

# Script generated for node Join
Join_node1716514721539 = Join.apply(frame1=accelerometer_landing_node1716514553725, frame2=customer_trusted_node1716514431042, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1716514721539")

# Script generated for node Drop Fields
DropFields_node1716514796742 = DropFields.apply(frame=Join_node1716514721539, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1716514796742")

# Script generated for node Drop Duplicates
DropDuplicates_node1716569511834 =  DynamicFrame.fromDF(DropFields_node1716514796742.toDF().dropDuplicates(["serialnumber"]), glueContext, "DropDuplicates_node1716569511834")

# Script generated for node customer_curated
customer_curated_node1716514839199 = glueContext.getSink(path="s3://olaitans3bucket/Customer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1716514839199")
customer_curated_node1716514839199.setCatalogInfo(catalogDatabase="abo_wells",catalogTableName="customer_curated")
customer_curated_node1716514839199.setFormat("json")
customer_curated_node1716514839199.writeFrame(DropDuplicates_node1716569511834)
job.commit()
