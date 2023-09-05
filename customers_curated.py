import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1693492337044 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1693492337044",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="customer_trusted", transformation_ctx="S3bucket_node1"
)

# Script generated for node Join
Join_node1693492360927 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1693492337044,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693492360927",
)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=Join_node1693492360927,
    mappings=[
        ("serialnumber", "string", "serialnumber", "string"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("birthday", "string", "birthday", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("phone", "string", "phone", "string"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

# Script generated for node S3 bucket 2
S3bucket2_node3 = glueContext.getSink(
    path="s3://stedi-722687989925/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket2_node3",
)
S3bucket2_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
S3bucket2_node3.setFormat("json")
S3bucket2_node3.writeFrame(ChangeSchema_node2)
job.commit()
