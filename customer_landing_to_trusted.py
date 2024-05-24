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

# Script generated for node customer_landing
customer_landing_node1716506050703 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://olaitans3bucket/Customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1716506050703")

# Script generated for node Filter-research
Filterresearch_node1716506134394 = Filter.apply(frame=customer_landing_node1716506050703, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filterresearch_node1716506134394")

# Script generated for node customer_trusted
customer_trusted_node1716506158756 = glueContext.getSink(path="s3://olaitans3bucket/Customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1716506158756")
customer_trusted_node1716506158756.setCatalogInfo(catalogDatabase="abo_wells",catalogTableName="customer_trusted")
customer_trusted_node1716506158756.setFormat("json")
customer_trusted_node1716506158756.writeFrame(Filterresearch_node1716506134394)
job.commit()
