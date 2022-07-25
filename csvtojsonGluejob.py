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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://week-3bucket/input/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "bigint", "id", "long"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("date_of_birth", "string", "date_of_birth", "string"),
        ("ethnicity", "string", "ethnicity", "string"),
        ("gender", "string", "gender", "string"),
        ("status", "string", "status", "string"),
        ("entry_academic_period", "string", "entry_academic_period", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://week-3bucket/output/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
