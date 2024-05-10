import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col  # Make sure this import is included

# Get the arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create a DynamicFrame from the Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="cleandata",
    table_name="cleaned_crashes_csv",
    transformation_ctx="AWSGlueDataCatalog_node1714138602249"
)

# Convert to DataFrame to handle transformations not supported by DynamicFrame
df = dynamic_frame.toDF()


# Convert DataFrame back to DynamicFrame for compatibility with AWS Glue operations
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "final_df")

# Write DataFrame to RDS
target_database = "crash_data"
target_table = "crashes"
target_connection_options = {
    "url": "jdbc:mysql://database-1.cvm4a28igxw6.us-east-1.rds.amazonaws.com:3306/" + target_database,
    "dbtable": target_table,
    "user": "admin",
    "password": "Lokesh1999",
    "database": target_database
}

# Write DataFrame to RDS
df.write.jdbc(
    url=target_connection_options["url"],
    table=target_connection_options["dbtable"],
    mode="overwrite",
    properties={
        "user": target_connection_options["user"],
        "password": target_connection_options["password"]
    }
)

# Commit the job
job.commit()
