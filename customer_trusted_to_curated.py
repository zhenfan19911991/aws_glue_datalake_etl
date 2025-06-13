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
AWSGlueDataCatalog_node1749794607712 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1749794607712")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749794596146 = glueContext.create_dynamic_frame.from_catalog(database="zhen_project", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1749794596146")

# Script generated for node SQL Query
SqlQuery6738 = '''
select distinct a.*
from customer_trusted a
join accelerometer_trusted b on a.email = b.user

'''
SQLQuery_node1749794625592 = sparkSqlQuery(glueContext, query = SqlQuery6738, mapping = {"customer_trusted":AWSGlueDataCatalog_node1749794607712, "accelerometer_trusted":AWSGlueDataCatalog_node1749794596146}, transformation_ctx = "SQLQuery_node1749794625592")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749794625592, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749794592778", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749794627916 = glueContext.getSink(path="s3://zhen-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749794627916")
AmazonS3_node1749794627916.setCatalogInfo(catalogDatabase="zhen_project",catalogTableName="customers_curated")
AmazonS3_node1749794627916.setFormat("json")
AmazonS3_node1749794627916.writeFrame(SQLQuery_node1749794625592)
job.commit()