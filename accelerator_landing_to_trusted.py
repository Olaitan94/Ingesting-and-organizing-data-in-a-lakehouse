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

# Script generated for node customer_trusted
customer_trusted_node1716511477733 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://olaitans3bucket/Customer/trusted/run-1716506352248-part-r-00000"], "recurse": True}, transformation_ctx="customer_trusted_node1716511477733")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1716511701726 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://olaitans3bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_landing_node1716511701726")

# Script generated for node Join
Join_node1716511808985 = Join.apply(frame1=Accelerometer_landing_node1716511701726, frame2=customer_trusted_node1716511477733, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1716511808985")

# Script generated for node Drop Fields
DropFields_node1716511835958 = DropFields.apply(frame=Join_node1716511808985, paths=["serialNumber","shareWithPublicAsOfDate","birthDay","registrationDate","shareWithResearchAsOfDate","customerName","email","lastUpdateDate","phone","shareWithFriendsAsOfDate",], transformation_ctx="DropFields_node1716511835958")

# Script generated for node Accelerometer
Accelerometer_node1716512119523 = glueContext.getSink(path="s3://olaitans3bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_node1716512119523")
Accelerometer_node1716512119523.setCatalogInfo(catalogDatabase="abo_wells",catalogTableName="Accelerometer_trusted")
Accelerometer_node1716512119523.setFormat("json")
Accelerometer_node1716512119523.writeFrame(DropFields_node1716511835958)
job.commit()
