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


# Script generated for node Customer Curated
CustomerCurated_node1688102900198 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://olaitans3bucket/Customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1688102900198",
)


# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://olaitans3bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node Change Schema
ChangeSchema_node1716527262446 = ApplyMapping.apply(frame=CustomerCurated_node1688102900198, mappings=[("serialNumber", "string", "customer_serialNumber", "string"), ("birthDay", "string", "birthDay", "string"), ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"), ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"), ("registrationDate", "bigint", "registrationDate", "bigint"), ("customerName", "string", "customerName", "string"), ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint"), ("email", "string", "email", "string"), ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1716527262446")

# Script generated for node Join Step Trainer
JoinStepTrainer_node1688103008151 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=ChangeSchema_node1716527262446,
    keys1=["serialNumber"],
    keys2=["customer_serialNumber"],
    transformation_ctx="JoinStepTrainer_node1688103008151",
)

# Script generated for node Drop Fields
DropFields_node1716522960010 = DropFields.apply(frame=JoinStepTrainer_node1688103008151, paths=["customer_serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",], transformation_ctx="DropFields_node1716522960010")

# Script generated for node Drop Duplicates
DropDuplicates_node1716563924028 = DynamicFrame.fromDF(
    DropFields_node1716522960010.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1716563924028",
)

# Script generated for node Step Trainer Curated Zone
StepTrainer_trusted_node3 = glueContext.getSink(
    path="s3://olaitans3bucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainer_trusted_node3",
)
StepTrainer_trusted_node3.setCatalogInfo(
    catalogDatabase="abo_wells", catalogTableName="step_trainer_trusted"
)
StepTrainer_trusted_node3.setFormat("json")
StepTrainer_trusted_node3.writeFrame(DropDuplicates_node1716563924028)
job.commit()
