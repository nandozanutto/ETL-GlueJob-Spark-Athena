import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1693513595512 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="AmazonS3_node1693513595512",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1693516198669 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1693516198669",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1693522947037 = DynamicFrame.fromDF(
    AmazonS3_node1693513595512.toDF().dropDuplicates(["serialnumber"]),
    glueContext,
    "DropDuplicates_node1693522947037",
)

# Script generated for node Join
Join_node1693513598216 = Join.apply(
    frame1=RenamedkeysforJoin_node1693516198669,
    frame2=DropDuplicates_node1693522947037,
    keys1=["right_serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1693513598216",
)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=Join_node1693513598216,
    mappings=[
        ("serialnumber", "string", "serialnumber", "string"),
        ("right_sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("right_distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-722687989925/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ChangeSchema_node2)
job.commit()
