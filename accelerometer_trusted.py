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

# Script generated for node Customer Trusted
CustomerTrusted_node1693360025269 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1693360025269",
)

# Script generated for node Accelerometer
Accelerometer_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1693360318475 = Join.apply(
    frame1=Accelerometer_node1,
    frame2=CustomerTrusted_node1693360025269,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1693360318475",
)

# Script generated for node Change Schema
ChangeSchema_node1693363672402 = ApplyMapping.apply(
    frame=CustomerPrivacyFilter_node1693360318475,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "long", "timestamp", "long"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
    ],
    transformation_ctx="ChangeSchema_node1693363672402",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stedi-722687989925/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ChangeSchema_node1693363672402)
job.commit()
