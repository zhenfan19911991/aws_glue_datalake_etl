import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749792689407 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1749792689407")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749792704809 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1749792704809")

# Script generated for node SQL Query
SqlQuery7231 = '''
select distinct b.*
from customer_trusted a 
join accelerometer_landing b on a.email = b.user

'''
SQLQuery_node1749792693424 = sparkSqlQuery(glueContext, query = SqlQuery7231, mapping = {"customer_trusted":AWSGlueDataCatalog_node1749792689407, "accelerometer_landing":AWSGlueDataCatalog_node1749792704809}, transformation_ctx = "SQLQuery_node1749792693424")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749792693424, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749792457352", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749792695678 = glueContext.getSink(path="s3://zhen-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749792695678")
AmazonS3_node1749792695678.setCatalogInfo(catalogDatabase="zhen_project",catalogTableName="accelerometer_trusted")
AmazonS3_node1749792695678.setFormat("json")
AmazonS3_node1749792695678.writeFrame(SQLQuery_node1749792693424)
job.commit()